# ADR-0016: Binary API shared memory and SVM queue ownership

- Date: 2026-09-14
- Status: Approved for implementation in Issue #308; behavior and performance not yet verified
- Scope: `hammer-infra::svm::{queue,region}`, `hammer-ipc::binary_api`
- Reference: vendored `third_party/vpp/src/vlibapi/memory_shared.{c,h}` and
  `third_party/vpp/src/svm/queue.{c,h}`

## 1. Decision and boundary

Implement the shared-memory Binary API transport's **behavior, ownership and
cacheline costs**, not a C ABI. The existing generic `SvmQueue` must itself
become the shared queue object. It is initialized at the beginning of one
64-byte-aligned SVM allocation, immediately followed by runtime-sized queue
elements; the ring records and the input-queue field point to that object.
Remove the process-local `SvmQueue` handle and its separate shared header
type. A pointer to a local handle cannot stand for the queue
pointer in `ring_alloc_t`.

Keep the existing fixed-VA `SvmRegion`, region mutex, PVT/Data Heap and
`MemHeap::activate()`. Keep `SvmRegionHeader` cacheline-aligned: it controls
mapping and is not a message ring. This is intentionally different from
copying the C region header. No C source, size probe, foreign vector header,
or cross-language layout test is added. The in-region `Vec<RingAlloc>` tables
are Hammer-only, fixed-VA, same-build shared storage allocated on the Data
Heap before clients can read them; they are **not** claimed to have VPP vec
layout. Do not put `data: [u8; 0]` in any Rust type.

This ADR supersedes the process-local queue-handle and typed-adapter parts of
ADR-0011 section 6.1; it retains ADR-0011/0012's region-owned mapping and
heap rules. `SvmMsgQ` has a distinct descriptor/ring layout and is not
converted into this queue. This is a design only: no handler, client
registration, codec replacement or test for unfinished Binary API shared
memory is introduced. Hammer's existing `ApiMain` is **not yet global**;
installation of that same type and an owner-local accessor are part of this
proposal, without adding a handler parameter or another main type.

## 2. Source evidence and semantic diff

Paths in the table are relative to `third_party/vpp/src/` unless prefixed
`crates/`. V rows follow the vendored implementation and its call paths;
H rows describe the inspected Hammer implementation, not proposed behavior.

| ID | Verified source and call path | Consequence |
| --- | --- | --- |
| V1 | `svm/queue.h:10-29`, `svm/queue.c:20-84,249-420` | The mutex, condvar, head/tail, volatile occupancy, capacity, fd numbers and element bytes occupy one allocation; add/sub lock that same mutex and copy exactly `elsize` bytes into/out of the queue slot. `svm_queue_alloc_and_init` requests a 64-byte-aligned allocation, but the queue type does not require 64-byte struct alignment. |
| V2 | `vlibapi/memory_shared.h:21-94`, `vlibapi/memory_shared.c:35-175` | Each ring record holds `svm_queue_t *rp`, `u16` size/count and hit/miss counters. Allocation checks the current head; a busy eligible class is a miss and the next class is tried **until its GC mark passes ten seconds** (V6). Server rings require the main thread; client rings lock their queue. All classes exhausted means Data Heap allocation. |
| V3 | `vlibapi/memory_shared.c:189-255,494-518` | Ordinary allocation compares the current process PID with the shared server PID *on every call*. Equal selects server rings, unequal selects client rings; forced-client calls bypass the comparison. |
| V4 | `vlibapi/api_common.h:127-134`, `vlibapi/memory_shared.c:259-326,397-410,740-783`, `svm/queue.c:277-320`, `vlibapi/api_shared.c:970-989` | The message prefix's `q` marks the allocation ring (free checks only whether it is null), not the input queue. A Binary API input slot has size `sizeof(uword)`; send passes the *address of the message address* to waiting `svm_queue_add(q, elem, 0)`. The queue copies that one address, never the message payload. Free clears the ring marker or returns heap storage to the originating region's Data Heap; `can_send` only observes capacity. |
| V5 | `vlibapi/memory_shared.c:397-520,524-736`, `vlibapi/api_common.h:246-289`, `svm/svm.h:58-69` | Server switches to the Data Heap for header, ring-table and queue initialization, restores its previous heap, then sets `user_ctx` before unlocking the region. The process-local API main retains the current region pointer, shared-header pointer, own PID and ring-miss count; ordinary mapping also records the region in `mapped_shmem_regions` for unmap. Client waits at most 100 s for the file and header. Restart scans exited clients in the API region and root, changes PID and drains the input queue. |
| V6 | `vlibapi/memory_shared.c:86-145,598-643` | A busy head gets a seconds timestamp on first inspection; when `now - mark > 10`, VPP reuses it, increments `garbage_collects` and clears its mark. On restart it tries the input mutex ten times, sleeping 10 ms after each failed attempt, then zeroes the mutex bytes if none succeeded; only then does it drain and free old messages. |
| V7 | `vlibapi/api_shared.c:570-633,792-806`, `svm/queue.c:374-421` | Receive dequeues the address into a stack `uword`, reads the prefix length, then processes the message. The dispatch call's `free_it` flag decides whether processing frees it; a no-free handler path exists. The queue copies out the address, not the payload, and does not free message storage. |
| V8 | `vlibapi/memory_shared.c:699-738`, `svm/svm.c:1000-1138` | Ordinary unmap relinquishes each entry in `mapped_shmem_regions`; the SVM region's mapper list determines whether the last mapper releases the region. It does not iterate `vlib_private_rps`, destroy individual ring queues on each detach, or wait for all remote clients before the current process unmaps. |
| V9 | `svm/queue.c:20-84`, `vlibapi/memory_shared.c:88-140,397-436` | Queue initialization clears the queue header, while the ring allocator immediately checks each slot's `msgbuf_t.q` marker. Before Hammer performs atomic loads on those markers, ring initialization must explicitly establish the same initially free state for every slot. |
| V10 | `vlibapi/api_shared.c:23-36`, `vlibapi/api_common.h:382-401`, `vlibapi/memory_shared.c:35-46,189-255,259-305`, `vlibmemory/memory_client.c:48-60`, `vcl/vcl_bapi.c:499-513` | VPP's default main is static; `vlibapi_get_main()` reads a thread-local pointer initialized to that default. Client receive threads and VCL workers can instead select their own API main. Ordinary allocation/free fetch the selected main once per operation and use its current region. Hammer selects the same `ApiMain` type per thread; implementing VPP client workers and their lifecycles remains outside this ADR. |
| V11 | `vlibapi/memory_shared.c:54-60,156-185,397-409,441-479,687-689`, `vlibmemory/socket_api.c:714-717`, `svm/queue.c:75-84` | Allocation records the requested length in network order before returning; the **internal explicit-region** allocation can return null for a missing `user_ctx` even when non-nullable. Ordinary map always uses default rings and a 1024-entry pointer queue unless the API main overrides its length. Custom element configuration belongs to private-region initialization. Queue free destroys mutex/condvar **and** releases its allocation. |
| V12 | `vlibapi/api_shared.c:970-989`, `vlibapi/memory_shared.c:258-329,636-643` | Normal heap release locks the region, switches to its Data Heap, frees, restores the previous heap and unlocks. Restart already holds the region lock: its private no-lock release switches heaps and frees without locking again. |
| V13 | `vlibapi/api_common.h:274-288`, `vlibmemory/memory_api.c:31-59,531-535,801-829,1069-1090`, `vlibmemory/socket_api.c:678-724`, `vlibapi/memory_shared.c:703-720` | The current region and cached header can be temporarily selected for a private message and then restored. The primary pointer is set separately after main API setup and remains the reference to the main region. Private regions have their **own** memfd-backed pointer vector and lifetime (including a leading memfd header page). The ordinary mapped-region vector is separate and is the one iterated by shared-memory unmap. |
| V14 | `vlibapi/api_common.h:305-324`, `vlibapi/memory_shared.c:356-393,579-591`, `svm/svm.c:879-895`, `vlibmemory/memory_api.c:1157-1170`, `vlibmemory/socket_api.c:699-708`, `svm/svm_common.h:26,92-94` | VPP's root uses `global_size` (64 MiB total by default) and `global_pvt_heap_size` (128 KiB by default). Ordinary API `api_size` defaults to **16 MiB of data**: `svm_region_find_or_create` adds a header page and the `api_pvt_heap_size` (zero selects 128 KiB) before page rounding. The private memfd path also reads `api_pvt_heap_size`. Root base VA and backing uid/gid belong to root/file setup. |
| V15 | `vlibapi/memory_shared.c:610-649`, `svm/svm.c:1209-1230` | Restart scans the API region while locked and the root only after the API region is unlocked. VPP discards a non-self, nonzero client PID on **any** failed `kill(pid, 0)`, with no returned probe error. Root cleanup occurs after the input queue has already been drained. |
| V16 | `vlibapi/memory_shared.c:35-77,188-255`, `vlibapi/memory_shared.h:98-105` | The internal allocator accepts signed `int nbytes` and returns null on a missing region header, but ordinary entry points dereference their cached header before calling it. For a valid header, only nullable allocation can return null on Data Heap exhaustion; ordinary allocation uses non-returning allocation. |
| V17 | `vlibapi/memory_shared.c:494-518`, `vlibapi/memory_shared.h:94`; scoped search for `VL_SHM_VERSION` under `third_party/vpp/src/` | VPP writes a shared-header version but does not check it on this mapping/attach path. Hammer's **existing** `SvmRegion` version check (`crates/hammer-infra/src/svm/region.rs:823-830`) is the boundary for rejecting a previous queue layout. Do not invent an additional header-version error. |
| V18 | `vlibapi/api_shared.c:23-27`, `vlibapi/api_common.h:305-324`, `vlibapi/memory_shared.c:340-393,579-591`, `vlibmemory/memory_api.c:1157-1170`, `svm/svm.c:49-86,561-569,614-626`, `vpp/api/api.c:278-310` | API UID/GID default to `-1`, are supplied for both root and ordinary API backing, and a failed backing `fchown` is a warning, not a map error. A zero root base selects VPP's Linux x86_64 default `0x130000000`; global size is the root's total mapping, API size is subregion data bytes. |
| H4 | `crates/hammer-infra/src/svm/region.rs:73-161,413-483,552-564,670-675` | `SvmRegionConfig::size` is the **total** size for `SvmRegion::create` but is the **data payload** size for `find_or_create_subregion`, which adds its header page and PVT Heap. Passing VPP's 16 MiB API **data** size directly to the Hammer subregion path preserves the intended data budget. Both paths already accept `pvt_heap_size`, with zero selecting Hammer's 128 KiB default. |
| H1 | `crates/hammer-infra/src/svm/queue.rs:97-225,335-430,585-632` | Today the queue header is shared and `SvmQueue` is a local pointer/borrowed-lock/owned-fd bundle. The header's `align(64)` pads before the elements. `SvmQueueElements<T>` owns that local bundle and allocates a `Vec` on every typed receive. There is no ring-head operation. |
| H2 | `crates/hammer-infra/src/svm/region.rs:165-184,375-404,413-518`, `crates/hammer-infra/src/mem/mod.rs:735-758,891-906` | The region already owns the shared `user_ctx`, mapping, RAII lock and Data Heap. `ActiveHeap` restores the thread's earlier heap on Drop. The region currently has no public method to set/read `user_ctx`. |
| H3 | `crates/hammer-ipc/src/binary_api/api.rs:19-88`, `crates/hammer-ipc/src/binary_api/codec.rs:26-37` | `ApiMain` is currently a constructed value with `&mut self` registration methods, **not** an installed global. `Api` supplies Serde serialization/deserialization. The codec currently requires an initialized `&mut [u8]`; a nonzeroing serializer is not designed yet. |
| H5 | `crates/hammer-infra/src/svm/region.rs:370-402,530-549,1302-1312` | The existing `SvmRegion::remove_exited_clients` takes the region lock itself. The scan must move onto `RegionLock` to avoid recursive locking during restart. Unlike VPP, Hammer removes only `ESRCH`, retains `EPERM`, and returns `ClientProbe` on another OS error. This is an intentional difference preserving the existing generic SVM error contract. |
| H6 | `crates/hammer-infra/src/svm/region.rs:1004-1054` | `RegionLock` already creates a short-lived `posix-sync::BorrowedMutex` and binds its guard lifetime to its mapped owner after acquisition. A queue guard can use this existing approach for ordinary queue lock/wait/unlock; only VPP's forced mutex reset needs raw pthread access. |
| H7 | `crates/hammer-infra/src/svm/region.rs:73-161,413-428,552-565`, `crates/hammer-infra/tests/svm_region.rs:73-86` | Existing `SvmRegion::create` takes a caller-supplied fixed VA and `OwnedFd`; `find_or_create_subregion` also takes an `OwnedFd`. Neither `SvmRegionConfig` nor those entry points has UID/GID fields. The backing creator must apply ownership to its descriptor, using `ApiMain`'s configured values. |

| Dimension | Current Hammer | Final design | Evidence |
| --- | --- | --- | --- |
| Queue ownership | Shared header plus local queue bundle | Shared `SvmQueue` itself; mapping owner lends `&SvmQueue` | V1, H1 |
| Queue layout | 64-byte-aligned header and padded element start | 64-byte-aligned allocation base, naturally aligned queue, elements immediately after `size_of::<SvmQueue>()` | V1, H1 |
| Lock and occupancy | Local borrowed POSIX objects, plain shared `cursize` | Shared raw POSIX storage, queue-bound RAII guard, atomic occupancy for lockless hints | V1, H1 |
| Ring record | No Binary API rings | `NonNull<SvmQueue>` for each ring and input queue, not local handles | V2 |
| Allocation | No shared message allocator | Global `ApiMain` supplies the current PID/region internally; PID-selected ring table, next size class on busy or force-reuse after ten seconds, region Data Heap last | V2, V3, V6, V10 |
| Message | Codec only | `MsgBuf` holds a private payload address and byte counts, **not** a stored Rust slice or region reference; unsafe, operation-scoped payload access and explicit release account for forced reuse | V4, V6, V7, V10, H3 |
| Enqueue/dequeue | Byte queue | Input queue copies only a pointer-sized address; the payload remains in its original ring slot or Data Heap allocation | V1, V4, V7 |
| Region roles | No Binary API mapped-region state | Separate current and primary pointers, private-region pointers, and ordinary mapped-region owners; the private memfd mapping is not an ordinary SVM subregion | V5, V8, V13 |
| Region geometry and backing | Root and child use `SvmRegionConfig`; root base and backing fd are supplied by their caller | Root base, total size, PVT and backing owner; API data size, PVT and backing owner are independent; pass API data bytes directly to `find_or_create_subregion` | V14, V18, H4, H7 |
| Main ownership | `ApiMain::new` exists but no installed global | Install the default main once; a thread-local `&'static ApiMain` initially selects it and may select another same-type main for that thread. Selection does not own or synchronize the main or its mapping. Statistics remain atomic. | V10, H3 |
| Lifecycle | Region already maps and locks | Initialize under Data Heap; set `user_ctx` after restore; attach validates before access; restart tries then forcibly resets the input mutex and drains; root PID scan is advisory after restart; unmap without per-queue destruction | V5, V6, V8, V15, H2, H5 |
| ABI | Hammer-only fixed-VA shared objects | Preserve same-build contract, reject old mapped layout; no C compatibility claim | H1, H2 |

## 3. Types and method ownership

These are **proposed Rust signatures**, not implemented code. The VPP names
in the left column are source citations, never Hammer identifier prefixes.
There is no `ApiSharedMemory`, `ClientRegistration`, message dispatch, or
handler type in this proposal.

| VPP concept | Hammer owner |
| --- | --- |
| `svm_queue_t` | shared `hammer-infra::svm::queue::SvmQueue` |
| `ring_alloc_t` | private `hammer-ipc::binary_api::memory_shared::RingAlloc` |
| `vl_shmem_hdr_t` | `hammer-ipc::binary_api::memory_shared::ShmemHeader` |
| `vl_api_shm_elem_config_t` | `ShmElemConfig`/`ShmElement` belong to the deferred private-region path; ordinary mapping has no custom-config argument |
| `msgbuf_t` | `MsgBuf`: private payload address/length handle, without a stored Rust borrow, region or input queue; the three shared prefix fields have no separate Rust struct name |
| `api_main_t.our_pid/vlib_rp/vlib_primary_rp/vlib_private_rps/mapped_shmem_regions/shmem_hdr/ring_misses/vlib_input_queue_length` | existing `hammer-ipc::binary_api::api::ApiMain`, extended with distinct process-local region roles |
| `svm_region_t.user_ctx` | existing `hammer-infra::svm::region::{SvmRegion,RegionLock}` |

### Generic queue in hammer-infra

```rust
// The shared object starts at an externally owned, 64-byte-aligned allocation.
// No zero-length element field and no process-local pointer fields.
#[repr(C)]
pub struct SvmQueue {
    mutex: UnsafeCell<MaybeUninit<RawMutexAlloc>>,
    condvar: UnsafeCell<MaybeUninit<RawCondvarAlloc>>,
    head: UnsafeCell<u32>,
    tail: UnsafeCell<u32>,
    cursize: AtomicU32,
    maxsize: u32,
    elsize: u32,
    consumer_pid: AtomicI32,
    producer_evtfd: AtomicI32,
    consumer_evtfd: AtomicI32,
}
// SAFETY: initialized geometry is immutable after publication; head/tail and
// POSIX state follow the queue mutex (or the private server-ring precondition),
// while lockless occupancy and PIDs are atomic. Mapping lifetime is external.
unsafe impl Send for SvmQueue {}
unsafe impl Sync for SvmQueue {}

pub struct SvmQueueLock<'queue> {
    queue: &'queue SvmQueue, // owns an acquired lock until Drop
    guard: StandardGuard<'queue>,
    conditional_wait: SvmQueueConditionalWait,
}

impl SvmQueue {
    pub fn size_to_alloc(config: &SvmQueueConfig) -> Result<usize, SvmQueueError>;
    pub unsafe fn init(
        base: NonNull<u8>, config: &SvmQueueConfig,
    ) -> Result<NonNull<Self>, SvmQueueError>;
    pub unsafe fn attach(
        base: NonNull<u8>, allocation_bytes: usize,
    ) -> Result<NonNull<Self>, SvmQueueError>;
    pub unsafe fn cleanup(&self); // POSIX destroy only; owner also frees allocation
    pub unsafe fn reset_mutex_for_restart(&self); // ten trylocks, then zero bytes

    #[inline]
    pub fn capacity(&self) -> usize;
    #[inline]
    pub fn element_size(&self) -> usize;
    #[inline]
    pub fn consumer_pid(&self) -> i32;
    pub fn set_consumer_pid(&self, pid: i32); // exclusive region-restart phase
    #[inline]
    pub fn can_send(&self) -> bool; // non-reserving, advisory only
    pub fn len(&self) -> Result<usize, SvmQueueError>;
    pub fn is_empty(&self) -> Result<bool, SvmQueueError>;
    pub fn is_full(&self) -> Result<bool, SvmQueueError>;
    pub fn lock(&self) -> Result<SvmQueueLock<'_>, SvmQueueError>;
    pub fn try_lock(&self) -> Result<SvmQueueLock<'_>, SvmQueueError>;
    pub fn add(&self, element: &[u8], wait: SvmQueueConditionalWait)
        -> Result<(), SvmQueueError>;
    pub fn add2(&self, first: &[u8], second: &[u8], wait: SvmQueueConditionalWait)
        -> Result<(), SvmQueueError>;
    pub fn sub(&self, element: &mut [u8], wait: SvmQueueConditionalWait)
        -> Result<(), SvmQueueError>;
    pub fn sub2(&self, element: &mut [u8]) -> Result<(), SvmQueueError>;
    pub unsafe fn set_producer_event_fd(&self, fd: BorrowedFd<'_>);
    pub unsafe fn set_consumer_event_fd(&self, fd: BorrowedFd<'_>);

    // A typed element is selected per operation, not stored in a local queue wrapper.
    pub fn add_element<T: SvmQueueElement>(
        &self, element: &T, wait: SvmQueueConditionalWait,
    ) -> Result<(), SvmQueueError>;
    pub fn sub_element<T: SvmQueueElement>(
        &self, wait: SvmQueueConditionalWait,
    ) -> Result<T, SvmQueueError>;
    pub fn try_sub_element<T: SvmQueueElement>(&self)
        -> Result<Option<T>, SvmQueueError>;

    // Unsafe: only the single server-main-thread ring owner may skip the mutex.
    #[inline(always)]
    pub unsafe fn ring_head_slot_unlocked(&self) -> NonNull<u8>;
    #[inline(always)]
    pub unsafe fn advance_ring_head_unlocked(&self);
}

impl SvmQueueLock<'_> {
    pub fn add_nolock(&mut self, element: &[u8]) -> Result<(), SvmQueueError>;
    pub fn add2_nolock(&mut self, first: &[u8], second: &[u8])
        -> Result<(), SvmQueueError>;
    pub fn sub_raw(&mut self, element: &mut [u8]) -> Result<(), SvmQueueError>;
    pub fn wait(&mut self) -> Result<(), SvmQueueError>;
    pub fn timed_wait(&mut self, timeout: Duration) -> Result<WaitOutcome, SvmQueueError>;
    #[inline(always)]
    pub fn ring_head_slot(&mut self) -> NonNull<u8>;
    #[inline(always)]
    pub fn advance_ring_head(&mut self);
}
impl Drop for SvmQueueLock<'_> {
    fn drop(&mut self); // unlock the mutex in this same shared SvmQueue
}
```

`SvmQueueElement` retains its current zerocopy bounds. Remove
`SvmQueueElements<T>` and its `PhantomData`; all typed operations check
`size_of::<T>() == element_size()` before touching a slot. Typed receive
copies directly into stack `MaybeUninit<T>`, assumes initialization **only
after** a successful full-slot copy, and creates no per-dequeue `Vec`.
`SvmQueue::sub` still accepts a caller-owned `&mut [u8]`; pointer queues
can use a pointer-sized stack slot without any typed wrapper. Keep the
existing `SvmQueueConditionalWait`, `SvmQueueConfig`, `SvmQueueError`
and byte-queue operations, preserving their pre/post-commit distinctions.

`init` and `attach` are unsafe storage-level entries, not SVM-region APIs:
the caller supplies an aligned allocation and proves that its full range is
valid and stays mapped while the queue is used. They return the shared queue
address, as VPP's `svm_queue_init` does, not a process-local queue handle or
a borrow spuriously tied to a particular `SvmRegion`. `attach` also rejects
invalid count, element size, head/tail and occupancy before any element
access. The mapping owner creates `&SvmQueue` borrows from that address only
within the mapping's actual lifetime. Neither a borrow nor `Drop` unmaps or
frees storage. `cleanup` only destroys the POSIX objects; unlike VPP's
`svm_queue_free`, it does **not** free the allocation. The region/Data Heap
owner must also deallocate the queue with its original allocation layout
when the allocation is retired and no mapper uses it. These two operations
together correspond to `svm_queue_free`. Neither is called when each Binary
API participant unmaps; `memory_shared.c` releases the whole region through
SVM mapper accounting.

`RawMutexAlloc` and `RawCondvarAlloc` are initialized with the existing
`posix-sync` process-shared robust builders. Their **borrowed objects cannot
be stored in the shared queue**: they contain local addresses. Ordinary
queue operations derive temporary `BorrowedMutex`/`BorrowedCondvar` values
from the shared fields and use the existing `posix-sync` lock, guard and wait
APIs. `SvmQueueLock` retains `&SvmQueue` and the acquired `StandardGuard`;
bind the guard lifetime to the queue borrow using the same reviewed unsafe
lifetime extension already used by `SvmRegion::lock_header` (H6). The caller
must keep that allocation mapped for the entire borrow. Do not reimplement
ordinary pthread lock/wait/unlock or invent a second error translation.
Condvar wait atomically releases and reacquires this mutex; each wakeup rechecks
occupancy. Ordinary owner death retains the existing
`OwnerDied`/`NotRecoverable` contract. The **restart-only**
`reset_mutex_for_restart` alone uses raw pthread operations, bypassing normal
lock recovery: while the region mutex is held and with no local queue guard,
try the input mutex ten
times; unlock immediately on a successful trylock, otherwise wait 10 ms
after each failed attempt. If all fail, zero the raw mutex storage exactly
as `memory_shared.c:619-633` does, without reinitializing its attributes.
This is Linux/POSIX implementation-dependent and does not prove a live peer
has stopped using the mutex. The function is unsafe and not a normal queue
lock-recovery policy. No generic spin lock is added.
Raw shared `head` and `tail` are accessed through `UnsafeCell` only while
locked or under the documented single-server-main-thread ring precondition.

The existing `SvmRegion::remove_exited_clients` takes its own lock. Move its
actual scan into `RegionLock::remove_exited_clients`, activating the same
root/API PVT Heap for the PID vector and restoring the previous heap on
return; the region method locks once and calls this guard method. Restart
calls the guard method on the **already locked** API region, then locks and
scans the root separately after releasing the API region lock. No nested
acquisition or second PID-scanning policy is introduced.

The queue keeps the two **numeric** event-fd slots, not `OwnedFd`. Its unsafe
setters require the caller to retain each borrowed descriptor for as long as
its numeric slot can be used, and to ensure only that process interprets it.
Descriptor ownership stays with the mapping/role owner in process-local state; a
shared integer is never turned into `OwnedFd` on attach and never assumed to
be valid in another process. Binary API queues use the existing condvar
path unless that process has actually installed/received its own valid
descriptors. Preserve the existing notification-after-commit error
classification; descriptor transfer is not designed in this ADR.

### Binary API shared state and message buffer

```rust
// hammer-ipc::binary_api::memory_shared
// The custom-config types belong to private-region initialization, not this
// ordinary mapping API. Their implementation is deferred with that path.
#[repr(C)] // fixes Hammer's hot-field order, not a foreign ABI
pub struct ShmemHeader {
    version: u32,
    server_pid: AtomicI32,
    input_queue: NonNull<SvmQueue>,
    server_rings: Vec<RingAlloc>,
    client_rings: Vec<RingAlloc>,
    application_restarts: AtomicU32,
    restart_reclaims: AtomicU32,
    garbage_collects: AtomicU32,
    socket_file_index: u32,
}
// SAFETY: queue/ring addresses and Vec metadata are fixed before user_ctx is
// set; only independent atomic PIDs/counters change after publication.
unsafe impl Send for ShmemHeader {}
unsafe impl Sync for ShmemHeader {}
#[repr(C)] // stable field placement inside Hammer shared storage
struct RingAlloc {
    queue: NonNull<SvmQueue>, // the shared queue object, not a local handle
    element_size: u16,
    capacity: u16,
    hits: AtomicU32,
    misses: AtomicU32,
}
// SAFETY: the queue address and size class do not change after publication;
// hit/miss statistics are atomic and the pointed-to queue is synchronized.
unsafe impl Send for RingAlloc {}
unsafe impl Sync for RingAlloc {}
pub struct MsgBuf {
    payload: NonNull<u8>, // private address, not a stored Rust reference
    payload_len: usize,
    initialized_len: usize, // local encoding state; never written to shared storage
}
enum RingRole { Server, Client }

impl ShmemHeader {
    #[inline]
    fn server_pid(&self) -> i32;
    fn set_server_pid(&self, pid: i32); // server create/restart
    #[inline]
    fn rings(&self, role: RingRole) -> &[RingAlloc];
    #[inline]
    pub unsafe fn input_queue(&self) -> &SvmQueue; // mapping and mutex must stay valid
}
// ADR-0017: the actual Main selects both rings and the fallback/free heap.
impl ApiMain {
    pub unsafe fn alloc(&self, payload_len: usize) -> MsgBuf;
    pub unsafe fn alloc_zeroed(&self, payload_len: usize) -> MsgBuf;
    pub unsafe fn alloc_or_null(&self, payload_len: usize) -> Option<MsgBuf>;
    pub unsafe fn alloc_as_client(&self, payload_len: usize) -> MsgBuf;
    pub unsafe fn alloc_zeroed_as_client(&self, payload_len: usize) -> MsgBuf;
    pub unsafe fn alloc_as_client_or_null(&self, payload_len: usize) -> Option<MsgBuf>;
    pub unsafe fn free(&self, message: MsgBuf);
}
impl RingAlloc {
    // A single eligible size class checks its current slot and owns hit/miss/GC accounting.
    #[inline(always)]
    unsafe fn alloc_slot(
        &self, allocation_bytes: usize, role: RingRole, garbage_collects: &AtomicU32,
    ) -> Option<MsgBuf>;
}
impl MsgBuf {
    #[inline]
    pub fn len(&self) -> usize; // process-local length; no shared-memory read
    pub unsafe fn encode<T: Api>(&mut self, value: &T) -> Result<usize, codec::Error>;
    pub unsafe fn decode<T: Api>(&self) -> Result<T, codec::Error>;
    // Restart already holds the originating region lock; never reacquire it.
    unsafe fn free_nolock(self, lock: &RegionLock<'_>);
}
impl From<&MsgBuf> for usize {
    fn from(message: &MsgBuf) -> usize; // payload address, not ownership transfer
}

// hammer-infra::svm::region: one explicit store and one corresponding load
impl SvmRegion {
    pub fn user_context(&self) -> Option<NonNull<u8>>;
    pub fn contains_range(&self, start: NonNull<u8>, bytes: usize) -> bool;
}
impl RegionLock<'_> {
    pub fn set_user_context(&mut self, value: NonNull<u8>);
    pub fn remove_exited_clients(&mut self) -> Result<usize, SvmRegionError>;
}

// hammer-ipc::binary_api::api: install the default owner once, select per thread.
#[allow(non_upper_case_globals)]
static api_global_main: OnceLock<ApiMain> = OnceLock::new();
thread_local! {
    #[allow(non_upper_case_globals)]
    static my_api_main: Cell<Option<&'static ApiMain>> = const { Cell::new(None) };
    // current() uses the installed default when no explicit selection exists.
}
pub struct ApiMain {
    // ADR-0017: main-thread API-init may populate these after installation.
    msg_data: RefCell<Vec<ApiMsgData>>,
    msg_id_by_name: RefCell<HashMap<&'static str, u16>>,
    msg_index_by_name_and_crc: RefCell<HashMap<String, u16>>,
    first_available_msg_id: Cell<u16>,
    msg_ranges: RefCell<Vec<ApiMsgRange>>,
    msg_range_by_name: RefCell<HashMap<String, usize>>,
    api_version_list: RefCell<Vec<ApiVersion>>,
    // Server registration pool and serialized table are specified by ADR-0017.

    // New, process-local shared-memory state:
    rp: UnsafeCell<Option<NonNull<SvmRegion>>>, // current region; not an owner
    primary_rp: UnsafeCell<Option<NonNull<SvmRegion>>>, // fixed primary region
    private_rps: UnsafeCell<Vec<NonNull<SvmRegion>>>, // private memfd pointers
    mapped_shmem_regions: UnsafeCell<Vec<Box<SvmRegion>>>, // ordinary owners
    shmem_header: UnsafeCell<Option<NonNull<ShmemHeader>>>, // follows current rp
    process_pid: AtomicI32,
    pub(super) ring_misses: AtomicU32,
    input_queue_length: u32, // 0 => default 1024
    api_uid: i32, // -1 => leave backing UID unchanged
    api_gid: i32, // -1 => leave backing GID unchanged
    global_base_va: u64, // 0 => target's default fixed root VA
    global_size: u64, // 0 => 64 MiB total root size
    api_size: u64, // 0 => 16 MiB API data bytes (plus region overhead)
    global_pvt_heap_size: u64, // 0 => SVM default
    api_pvt_heap_size: u64, // 0 => SVM default
    api_region_name: String, // ordinary API subregion name
}
// SAFETY: configuration is immutable after installation; all registry Cell/
// RefCell access first checks the runtime main thread (ADR-0017).
// Only the lifecycle owner mutates UnsafeCell fields, without concurrent
// allocator users; independent counters/PID are atomic. No mapping outlives
// its owner, and unsafe access is required while another process may GC.
unsafe impl Send for ApiMain {}
unsafe impl Sync for ApiMain {}
impl ApiMain {
    pub fn install(self); // once, after configuration and before map/API-init
    #[inline(always)]
    pub fn current() -> &'static Self; // selected main, defaulting to installed main
    pub unsafe fn set_main(main: &'static Self); // switches this thread only
    pub unsafe fn shmem_header(&self) -> &ShmemHeader; // mapped selected region
    pub fn set_input_queue_length(&mut self, length: u32); // before initial server map
    pub fn set_api_uid(&mut self, uid: i32);
    pub fn set_api_gid(&mut self, gid: i32);
    pub fn set_global_base_va(&mut self, base_va: u64);
    pub fn set_global_size(&mut self, bytes: u64);
    pub fn set_api_size(&mut self, bytes: u64);
    pub fn set_global_pvt_heap_size(&mut self, bytes: u64);
    pub fn set_api_pvt_heap_size(&mut self, bytes: u64);
    pub fn set_api_region_name(&mut self, name: String);
    pub fn api_uid(&self) -> i32;
    pub fn api_gid(&self) -> i32;
    pub fn global_base_va(&self) -> NonZeroUsize;
    pub fn root_region_config(&self) -> SvmRegionConfig;
    pub unsafe fn set_primary_region(&self); // primary API setup: primary_rp = rp
    pub unsafe fn map_shared_region(
        &self, root: &mut SvmRegion, path: &Path, is_server: bool,
    ) -> Result<(), MapError>;
    pub unsafe fn unmap_shared_regions(&self) -> Result<(), SvmRegionError>;
}

// Only mapping has a new boundary error. Queue and codec errors stay at
// their existing owners; ordinary allocation does not return an OOM error.
pub enum MapError {
    Open { path: PathBuf, #[source] source: io::Error },
    OpenTimeout { waited: Duration, #[source] source: io::Error },
    ReadyTimeout { waited: Duration },
    Region { #[source] source: SvmRegionError },
    Queue { #[source] source: SvmQueueError },
}
```

`ApiMain` field mapping and initialization are explicit:

| New field | VPP source and meaning | Lifecycle |
| --- | --- | --- |
| `rp: UnsafeCell<Option<NonNull<SvmRegion>>>` | `api_main_t.vlib_rp`; points to the **currently selected** process-local region descriptor, not a vector | Ordinary mapping selects that mapped API region; primary setup separately records it in `primary_rp`. A future private path may temporarily select a private region. Never owns either. Restart selects its region before draining old messages. |
| `primary_rp: UnsafeCell<Option<NonNull<SvmRegion>>>` | `api_main_t.vlib_primary_rp`; stable pointer to the primary API region | `set_primary_region` assigns the currently selected `rp` after primary API initialization (`memory_api.c:531-535`), not on every `map_shared_region` call; unaffected by temporary private-region selection. Cleared before its owning ordinary mapping is unmapped. Calling it requires an installed current ordinary region and exclusive lifecycle access. |
| `private_rps: UnsafeCell<Vec<NonNull<SvmRegion>>>` | `api_main_t.vlib_private_rps`; indexes private, socket/memfd-backed regions | Initially empty; populated only when the distinct private memfd path owns a stable full mapping including its header page. This vec is **not** an owner, not filled by ordinary map, and not drained by ordinary `unmap_shared_regions`. Private mapping and removal are outside this ADR. |
| `mapped_shmem_regions: UnsafeCell<Vec<Box<SvmRegion>>>` | `api_main_t.mapped_shmem_regions`; VPP's list for ordinary shared-memory unmap | Initially empty; owns the ordinary SVM mappings added by this map path. `Box` keeps their descriptors stable when the vec grows. Unmap explicitly calls `SvmRegion::unmap` for each entry. This vec does **not** own private memfd mappings. |
| `shmem_header: UnsafeCell<Option<NonNull<ShmemHeader>>>` | process-local `api_main_t.shmem_hdr`; cached header of the **current** `rp`, not always the primary header | Selected and restored together with `rp`; cleared before its current mapping is removed. Dereference only while that region is mapped. |
| `process_pid: AtomicI32` | `api_main_t.our_pid`; distinct from `ShmemHeader::server_pid` | Refreshed on mapping and read for pool selection with `Relaxed`; this atomic does not guard the region pointers. |
| `ring_misses: AtomicU32` | process-local `api_main_t.ring_misses`; counts fallback after all ring classes fail | Increments `Relaxed` once per Data Heap fallback, independently of shared per-ring hit/miss counters. |
| `input_queue_length: u32` | `api_main_t.vlib_input_queue_length`; zero selects 1024, nonzero overrides the default input-queue capacity | Initialized to zero by `ApiMain::new`; `set_input_queue_length` sets it before initial server map. Restart/attach uses the already mapped queue, not a new capacity. |
| `api_uid: i32` | `api_main_t.api_uid`; backing owner UID for the root and ordinary API regions | Defaults to `-1` (no UID change). `set_api_uid` configures it before installation. The backing-fd owner reads `api_uid()` for both root and API descriptors, including an already existing descriptor when correcting client-first ownership. |
| `api_gid: i32` | `api_main_t.api_gid`; backing owner GID for the same two regions | Defaults to `-1` (no GID change). `set_api_gid` configures it before installation; the backing-fd creator reads `api_gid()` alongside UID. |
| `global_base_va: u64` | `api_main_t.global_baseva`; fixed VA for the NODATA root | Zero selects `0x130000000` on ordinary Linux x86_64, matching `svm_get_global_region_base_va`. `set_global_base_va` configures it; the root creator passes `global_base_va()` to `SvmRegion::create`, not to the API subregion. |
| `global_size: u64` | `api_main_t.global_size`; root's total mapped bytes | Zero selects 64 MiB. Used by `root_region_config()` for the NODATA root; never treated as API subregion data bytes. |
| `global_pvt_heap_size: u64` | `api_main_t.global_pvt_heap_size`; root's PVT Heap | Zero selects the existing 128 KiB SVM default; used only when creating the NODATA root, not for the API Data Heap. |
| `api_size: u64` | `api_main_t.api_size`; API region's **data bytes**, before SVM overhead | Zero selects 16 MiB of data. Pass this directly as the `find_or_create_subregion` config `size`; VPP and Hammer both add header page and resolved API PVT Heap. |
| `api_pvt_heap_size: u64` | `api_main_t.api_pvt_heap_size`; ordinary API region's PVT Heap | Zero selects the same 128 KiB SVM default. The separate private memfd path will use this value too; it must not inherit `global_pvt_heap_size`. |
| `api_region_name: String` | `api_main_t.region_name`; name of the ordinary API segment | Defaults to `/vpe-api`, the value installed by VPP's daemon in `vpp/api/api.c:258`; VPP's generic main initially says `/unset`. Configured before installation; the caller's `path` is the backing-file path and does not silently replace the subregion's name. |

`ApiMain::new` initializes the region-role cells empty, counters and PID to
zero, UID/GID to `-1`, base VA and size/heap configuration to zero (the VPP
default sentinels), and
`api_region_name` to `/vpe-api`.
Setters take `&mut self` before installation. `install` stores the configured
ApiMain in the private `api_global_main`. `my_api_main` stores only a selection,
not a per-thread server replica; `current()` defaults to that installed object.
The daemon keeps its main thread on this default. Client workers, including the
future VCL use case, may select a distinct actual ApiMain with unsafe `set_main`.
The selected object must be `'static`; this does not grant permission to mutate
its region concurrently or to destroy its mapping while users remain.
The selector is the repository's Binary API TLS exception, not worker packet
state. It never selects a VAPI Client: each Client owns its connection/request
state and captures `current()` in a direct `&'static ApiMain` at construction.

ADR-0017 migrates ordinary allocation/free onto the actual ApiMain. Synchronous
implicit-owner handlers acquire `current()` at entry; owner methods and Client
operations use their existing reference. They do not reselect TLS during a
nested allocator operation or after an async wait. Switching TLS does not
redirect an existing Client, and is distinct from changing rp/header within its
Main. Daemon Process dispatch must keep the daemon Main selected.
API-init populates checked Cell/RefCell registry fields after installation;
range/version/table queries return actual Ref borrows. Message identities and
handlers stop changing once a serialized shared table has been published in the
current startup-only policy; this is not VPP's full runtime replacement policy.
Mapping/unmap use `&self`
and `UnsafeCell` only for lifecycle-owned pointer/vector mutation; they are
unsafe because callers must exclude simultaneous region-pointer access and
end all local message/queue operations before unmap. An `&mut ApiMain`
cannot be fabricated from the published shared reference. The pointer
`Option`s express VPP's nullable current/primary/header fields, while the
private and ordinary vectors start empty; neither `Option` nor `UnsafeCell`
is an ownership or cross-process synchronization protocol. During ordinary
mapping, `rp` points to a boxed `SvmRegion`: after mapping returns, its box
is owned by `mapped_shmem_regions`. On restart the local box stays alive
while `rp` is selected before the drain, then moves into the vec after the
drain (`memory_shared.c:636,656`); moving the box does not move its region
descriptor. `Box` and the vec live in process-local Main Heap storage, never
inside the shared region. An empty ordinary mapping list leaves `rp` and
`shmem_header` absent **unless** a separately owned private region is
selected. A mapped region can exist before its header is ready, but a
present header requires a live current `rp`. The cached header pointer
always follows the current `rp`; `primary_rp` remains on the primary region.
Ordinary `ApiMain::alloc*` uses self.shmem_header, self.rp, PID and counters
from the same owner. ShmemHeader supplies ring storage and RingAlloc owns the
per-class timestamp/hit/miss decision. `ApiMain::free(message)` verifies the
allocation belongs to self.rp before releasing a ring slot or activating that
region's Data Heap. Restart's MsgBuf::free_nolock retains its explicit region
lock and never reacquires it. No allocator/free method looks up TLS internally.
VPP's explicit registration/private-region allocator is outside this ordinary
mapping scope; it cannot be approximated by mixing a header from one Main with
a heap from another selected Main.
Ordinary `alloc`/`alloc_zeroed` and the forced-client non-nullable variants
return `MsgBuf`, not `Option<MsgBuf>`: the existing header reference rules out
the internal null-header branch, and exhausted Data Heap allocation does not
return. Only the two `*_or_null` methods return `Option` on heap exhaustion.

Private regions are not ordinary SVM subregions: `socket_api.c:678-724`
creates a memfd mapping with a leading header page, initializes the region
descriptor inside that mapping, and adds its address to `vlib_private_rps`.
Existing `SvmRegion::attach` and `SvmRegion::unmap` model the ordinary
mapping span; they cannot silently stand in for ownership of that full
private memfd mapping. The private socket/memfd owner and any needed generic
SVM mapping capability require a separate, approved design before
`private_rps` may contain entries. This ADR proposes no private-region
creation, registration, selection or removal method, and never treats
`mapped_shmem_regions` as the private-region owner.

The `root: &mut SvmRegion` argument to `map_shared_region` is Hammer's
existing NODATA root used to find or create an ordinary subregion. It is
**not** `primary_rp`: the primary API region is the mapped subregion, and
`rp` is whichever API region is currently selected. The root is borrowed
from its existing SVM owner, not added to `ApiMain` or either region vector.
`root_region_config()` returns `SvmRegionConfig { name: "/global_vm",
size: global_size_or_64_mib, pvt_heap_size: global_pvt_heap_size,
flags: NODATA }`. The root's owner supplies the fixed VA and backing fd to
the existing `SvmRegion::create`: call
`SvmRegion::create(api.global_base_va(), &api.root_region_config(), backing)`.
This is the counterpart of VPP's root
initialization in `memory_api.c:1157-1170`. The API map constructs its own
`SvmRegionConfig` with `flags: DATA_HEAP`, `name: api_region_name`,
`pvt_heap_size: api_pvt_heap_size` and
`size: api_size_or_16_mib`. Pass that `size` directly to
`root.find_or_create_subregion`: VPP's `svm_region_find_or_create`
(`svm.c:892-894`) and Hammer's method both add a header page and the
resolved API PVT Heap, then round to a page. The default API mapping therefore
occupies **16 MiB plus overhead**, while its requested data budget is 16 MiB.
The root's 64 MiB is a **total** mapping size and is not interpreted as a
subregion data budget. Zero PVT means `SVM_PVT_HEAP_SIZE` (128 KiB) in both
configs; the API size must still leave room for the Data Heap.
`api_pvt_heap_size` is
also read by VPP's private memfd initializer but `global_pvt_heap_size` is
never substituted for it. On Linux x86_64 convert the five `u64` base/size
values to `usize` without truncation before passing them to existing SVM
methods; `SvmRegion::create` still checks VA alignment and config sizes.
The root backing creator and ordinary API backing creator each obtain the
configured `api_uid()`/`api_gid()`, apply them to the backing fd (using `-1`
as the POSIX no-change sentinel), then pass that `OwnedFd` to the existing
SVM method. As in `svm.c:561-569,614-626`, an already existing backing may
also receive `fchown` to correct client-first ownership; a failed attempt is
an advisory diagnostic with the original OS error, not a `MapError` or a
reason to suppress mapping. File creation and fd ownership remain
with the existing root/API backing owners; `SvmRegionConfig` does not gain
UID/GID fields, and neither `SvmRegion` nor `MsgBuf` caches this policy.

`MsgBuf` stores neither a Rust payload borrow nor `ApiMain`/`SvmRegion`.
Because the main is global, its lifetime cannot prove the mapping stays
installed; `alloc*`, `input_queue`, encoding, decoding and free therefore
carry explicit unsafe preconditions. Calling `ApiMain::alloc*` requires
that its rp and header describe the same live mapping; missing setup is an
internal precondition violation, not successful absence.
The caller must keep the mapping and
the **originating** region installed while it accesses or frees a buffer,
and must not alias a message another process has reclaimed. No type alone
enforces this after VPP-style forced GC. The receiving process uses its own
current mapping through dequeue and any later free.
Input and ring queues are borrowed through the shared header: **no**
process-local queue `Vec` or separate queue pointer cache is added.
`map_shared_region` covers initial server setup, client attachment and the
existing-region server branch, as the single VPP map entry does. `root` is
required by the existing `SvmRegion::find_or_create_subregion`; the API
region config comes from the two API size/PVT fields, not another caller
parameter that could contradict them. `path` identifies the backing file
for opening/waiting. Ordinary mapping
always initializes the default rings and pointer-sized input queue; it does
not accept custom element configuration. VPP's custom configuration is passed
to private-region initialization from `socket_api.c:714-717`, which is outside
this ADR. There is no distinct restart entry point.
After `SvmQueue::sub` fills a pointer-sized stack slot, Binary API transport
code checks the payload and prefix ranges in its mapped region and constructs
the private `MsgBuf` handle. Range checking cannot prove exclusive control:
the producer must have stopped accessing the message at handoff, and a forced
GC may have reused this slot meanwhile. The caller of the unsafe decode/free
operations bears that non-aliasing precondition; the transport cannot
silently turn a checked address into a safe `&mut` borrow. An invalid shared
address is region corruption, not an ordinary empty-queue outcome; stop using
the region before dereferencing it.

`MsgBuf` is a non-owning address and byte-count handle, not a copy of a C
struct, a long-lived Rust slice, or an RAII queue owner. Its allocation contains a private
fixed prefix immediately before the payload:
`AtomicPtr<SvmQueue>` (null for Data Heap), `u32` network-order payload
length, and `AtomicU32` GC timestamp. The prefix has no Rust struct name or
zero-length array. Calculate its field offsets, payload offset, alignment
and total length with `Layout::extend` and checked arithmetic. Before any
slot lookup or Data Heap allocation, assert `payload_len <= i32::MAX as usize`:
VPP's allocation byte count is a signed `int`, and a larger Rust input has
no VPP counterpart. Check `prefix_bytes + payload_len` for overflow, check
ring slot size against this **total allocation size**, and convert the payload
length to shared network-order `u32` without truncation. Invalid length is a
caller-contract violation, not a nullable heap exhaustion or new error enum.
Allocation
and free use the properly aligned prefix internally. The prefix's ring
pointer is an occupied/free marker: VPP never dereferences it during free,
and it is **not** the destination queue. `MsgBuf` has no queue field, queue
parameter or queue method. `encode`/`decode` construct operation-scoped Rust
slices from the private address; those methods are unsafe because a peer can
force-reuse an occupied ring slot, or the mapping can be removed, while the
operation runs. `NonNull<u8>` is necessary here precisely because storing a
safe `&mut [MaybeUninit<u8>]` would promise exclusivity and lifetime the VPP
protocol cannot provide. `From<&MsgBuf> for usize` exposes only its payload
address as a queue-slot value, not a dereferenceable Rust reference or an
ownership transfer. Ring release clears the GC timestamp first, then clears the
marker with `Release`; the next allocator tests the marker with `Acquire`
before writing, so a previous release cannot reset a new owner's timestamp.
Heap release obtains the originating region from the receiver `ApiMain`, locks it and
deallocates on its Data Heap; the lifecycle precondition above requires that
region to be the originating region. `free` acquires the current region lock
only for heap allocations; the private `free_nolock` accepts the **already
held** `RegionLock` during restart, activates `lock.data_heap()`, frees and
restores the earlier heap before returning. Neither path reacquires the region
lock while holding it, and both leave a ring allocation's marker clear
without deallocating its ring storage. There is no safe payload-slice getter or
automatic `Drop`: just as in VPP, free is explicit. After enqueue, the
producer must not access the message; because generic `SvmQueue::add` copies
address bytes and does not consume `MsgBuf`, the type cannot enforce this.
`ApiMain::free(&self, message)` is unsafe: the type also cannot detect that another process has
received, freed or forcibly reclaimed that slot. VPP's ten-second threshold
is not an ownership certificate or a Rust aliasing guarantee.

The input queue's `element_size()` is `size_of::<usize>()`. Borrow the
*already allocated* buffer address with `From<&MsgBuf> for usize` and pass
its pointer-sized representation to waiting `SvmQueue::add`. Its
`elsize`-byte slot copy is a copy of the address, not of any payload byte;
the ring slot or Data Heap allocation stays where it was. No buffer-to-queue
trait or method on `MsgBuf` is needed:

```rust
let address = usize::from(&message);
queue.add(&address.to_ne_bytes(), SvmQueueConditionalWait::Wait)
```

The producer must relinquish control after the address is enqueued; neither dropping
`MsgBuf` nor completing `SvmQueue::add` frees the allocation. After dequeue,
the receiving process validates the address and length, then processes the
original payload in place under the unsafe access preconditions. Its processing policy
then chooses explicit free or no-free, matching VPP's `free_it` paths. A
generic queue error retains its existing pre/post-commit classification;
there is no extra Binary API automatic-free, retry or rollback path here.

`MsgBuf::encode<T: Api>` and `decode<T: Api>` are method-level generics.
There is no `PhantomData<T>`: `T` is not stored as a Rust object in shared
memory. Existing `Api` supplies Serde Serialize and Deserialize. Add
`codec::serialize_uninit<T: Serialize + ?Sized>(value, &mut [MaybeUninit<u8>])`
at the existing codec owner so encoding writes directly into an allocated
but uninitialized payload, without an intermediate `Vec`. Reuse the existing
`Serializer` implementation: its private output storage becomes
`&mut [MaybeUninit<u8>]`; the existing `Serializer::new(&mut [u8])` keeps its
public signature and internally reborrows initialized bytes as
`MaybeUninit<u8>`, while a private constructor accepts uninitialized output
for `serialize_uninit`. Writes initialize only the bytes actually produced;
do not manufacture an initialized slice of the unfilled tail. Every successful
allocation, **before returning**, writes the requested payload length to the
prefix in network byte order, including plain, zeroed and nullable variants;
plain allocation starts with `initialized_len = 0`, while zeroed allocation
starts at `payload_len`. Successful encoding updates `initialized_len` to
`max(initialized_len, encoded_bytes)`, without changing the shared requested
length. In particular, encoding fewer bytes into a zeroed buffer does not
uninitialize its tail. An encoding error leaves the requested shared length
unchanged and the caller explicitly
frees the still-controlled allocation. `decode` requires all `payload_len`
bytes to be initialized; otherwise return the existing `codec::Error` before
forming a Rust slice. `alloc_zeroed` initializes that range at allocation,
while plain `alloc` requires completed initialization before
queue handoff. Allocation takes a **byte count**, and the queue stores a
pointer-sized address internally: neither operation uses a `T` value, so
an unused method generic there would not implement typed behavior. This ADR
does not prescribe handler logic.

The default server ring budgets are `(64 + size_of::<RingAlloc>(), 1024)`,
`(256 + size_of::<RingAlloc>(), 128)`, `(1024 + size_of::<RingAlloc>(), 64)`;
client budgets are `(1024 + size_of::<RingAlloc>(), 1024)`,
`(2048 + size_of::<RingAlloc>(), 128)`, `(4096 + size_of::<RingAlloc>(), 8)`,
with `input_queue_length` pointer-sized input slots, using 1024 when that
process-local override is zero. These come from `memory_shared.h:53-61` and
`memory_shared.c:397-409`; they are allocator **policy**, not assertions about
C ABI. The default ring sizes include `size_of::<RingAlloc>()`, as VPP's
defaults include `sizeof(ring_alloc_t)`; use the actual Hammer record size.
The deferred private-region configuration branch likewise adds the record
size to each requested ring size, but it is not a parameter to ordinary map.
Validate capacity, pointer-width element size, slot alignment for the private
message prefix, fit and allocation arithmetic before setting `user_ctx`;
never truncate a configured capacity or element size.

## 4. Execution, synchronization and lifecycle

1. Server locks the region, takes its Data Heap from `RegionLock`, and
   activates it while creating the shared header, both fixed-length
   `Vec<RingAlloc>` tables and the actual `SvmQueue` objects.
   Drop `ActiveHeap` to restore the previous thread
   heap, then `RegionLock::set_user_context` stores the fully initialized
   header with `Release`, and finally unlock. Client reads
   `SvmRegion::user_context` with `Acquire`, checks region ranges, queue
   allocation bounds and ring-table bounds, then borrows
   `&SvmQueue`. The enclosing `SvmRegion` version check rejects previous
   queue layouts before accessing either shared header or queue. VPP writes
   its shared-header version but does not check it here (V17); Hammer stores
   a corresponding Hammer version without adding a second error family.
   The matching release/acquire orders **initialization**;
   it does not make unmapping safe while clients still hold references.
   Because the shared header contains Rust `Vec` values, unsafe attach requires
   a same-build published header whose `Vec` metadata already satisfies Rust's
   invariants; range checks can reject mismatched geometry but cannot safely
   interpret arbitrary corrupted bytes as `Vec` values.
   The mapping call records the current region, header address and process PID
   in `ApiMain`, as `memory_shared.c:503-505,598-601,679-683` does.
2. The actual `ApiMain` caches its process PID and current region when
   mapping. Each ordinary `ApiMain::alloc*` uses that same self
   and compares its PID to the shared `server_pid` at that moment. Equal means
   server rings and an assertion that the caller is the server's main thread
   through the existing runtime
   `ensure_main_thread()` check; unequal or forced-client means client
   rings and their mutex. `AtomicI32` replaces C's volatile PID: its relaxed
   load/store only select a pool, never establish header initialization or
   revoke an old server's access. On the existing-region server path the
   mapping call refreshes the process PID, as VPP does.
3. `ApiMain::alloc*` scans eligible `RingAlloc` records. Each record
   is visited in configured/default vector order, without sorting by size,
   and checks its queue head; clients use `SvmQueueLock::ring_head_slot`, and
   the sole server main thread uses the private unlocked operation. If the
   generic robust mutex reports `OwnerDied` after making itself consistent,
   the client ring reacquires it and continues, as VPP's ring lock does;
   other lock errors cannot be treated as a vacant ring slot. Busy
   eligible head gets `gc_mark_timestamp = time(0)` on first inspection.
   On subsequent inspection, `now.wrapping_sub(mark) > 10` forcibly reuses
   that **same** head, increments `ShmemHeader::garbage_collects`, then
   takes the ordinary hit/advance path without incrementing misses. At ten
   seconds or less, increment `misses` and try the next size class.
   Ring initialization sets each slot's marker to null before the header
   becomes visible; otherwise an atomic read of an uninitialized marker
   would be invalid. A free head sets its originating queue marker, clears
   the GC timestamp, increments `hits`, advances the ring head and returns a
   `MsgBuf` address handle. Atomic marker/timestamp updates avoid Rust data
   races on those **fields**; they cannot prevent a stale owner from reading,
   writing or clearing a reclaimed payload/marker. An undersized class is
   skipped without a miss. No queue `add/sub` occupancy transition occurs
   for ring storage. If all classes fail, drop any queue guard **before**
   taking the current region's lock from global `ApiMain` and allocating on
   its Data Heap. Heap-backed free uses that same current region while the
   originating mapping is still installed. The queue-to-region lock order is
   never reversed. Counters use `Relaxed` for statistics only.
4. The existing `SvmQueue::add(_, SvmQueueConditionalWait::Wait)` enqueues
   the pointer-sized element; `MsgBuf` has no queue dependency, and there is
   no Binary API send method on `MsgBuf` or an `ApiMain` send wrapper.
   `SvmQueue::can_send`
   reads `cursize` with `Relaxed` only as a full-queue hint; it cannot
   reserve a slot. Queue add/sub write head/tail under the robust mutex and
   update atomic occupancy under that mutex. POSIX mutex release/acquire
   orders pointer and payload handoff to the consumer; a standalone relaxed
   occupancy read does **not** make payload readable. If event-fd mode
   spins outside the mutex, occupancy requires a matching release store and
   acquire observation, or the wait must reacquire the mutex to inspect it.
   The consumer receives the address into a pointer-sized stack slot and
   validates its address/length against the mapping; message access remains
   unsafe even after validation, and decoding/dispatch remain outside this ADR.
5. Client file and header wait each allow 100 s in 10 ms steps. On a server
   restart, hold the API region mutex, read the existing `user_ctx`, scan
   exited API-region clients through `RegionLock::remove_exited_clients`,
   increment `application_restarts`, update the shared server PID and input queue
   consumer PID, then call `SvmQueue::reset_mutex_for_restart` on **only**
   the input queue (`memory_shared.c:598-635`). It tries the raw mutex ten
   times, waiting 10 ms on failure, unlocks immediately on success, and
   forcibly clears the mutex bytes after the tenth failure. Zeroed storage
   no longer has proven process-shared/robust attributes; subsequent
   interprocess synchronization is implementation-dependent. Record the
   current region in `ApiMain` **before** draining, as in
   `memory_shared.c:636`, so heap-backed old messages free against that
   region. Drain with
   nonwaiting queue `sub` until empty; free each dequeued message and increment
   `restart_reclaims` (`memory_shared.c:636-643`). The region lock remains
   held until the drain is done; each heap-backed free calls
   `MsgBuf::free_nolock(&region_lock)` to activate the region's Data Heap
   and restore the previous heap on exit. No nested region lock or input-queue
   guard survives into the heap free. Other queue mutexes are not reset.
   An error other than empty during
   drain cannot be treated as a dequeued address; the owner stops using that
   region instead of reading an uninitialized stack slot. Clearing a mutex
   held or used by a live peer and using a ring payload after forced reclaim
   have no Rust/POSIX safety guarantee. This ADR adopts VPP's behavior but
   **does not claim memory safety** when such peers continue accessing shared
   state; a later implementation must keep these operations behind explicit
   unsafe boundaries, not expose them through a safe borrow-based facade.
   After releasing the API region mutex, lock the root and scan its exited
   clients through the same `RegionLock` operation
   (`memory_shared.c:643-649`); this second scan is distinct from draining
   the API input queue. The root `&mut SvmRegion` is already supplied by
   the ordinary map operation. Hammer's existing scan removes only `ESRCH`,
   retains `EPERM` and can return `ClientProbe`, unlike VPP's remove-on-any-
   failed-`kill` rule (V15, H5). The API scan is **before** restart mutation,
   so a probe failure can return `MapError::Region` without claiming a restart
   took place. The root scan is **after** restart mutation and queue drain;
   root lock or probe failure is reported with its original source as an
   advisory diagnostic and the completed mapping returns success. It is not
   retried as if restart were uncommitted, and no fresh error variant is
   invented. Root scan failure can leave stale root PID entries; the next
   root scan can remove them.
6. The global main's owner unmaps only after local `MsgBuf` operations and
   queue borrows/guards end; this cannot be inferred merely from a borrow of
   the global main. With no private region selected, clear
   `ApiMain::shmem_header`, `rp` and `primary_rp` if they reference an ordinary
   mapping, before iterating
   `mapped_shmem_regions` and explicitly calling `SvmRegion::unmap(self)`
   for **each** boxed region. Dropping the box alone calls `Drop`, which only
   unmaps local pages and bypasses `SvmRegion::unmap`'s mapper accounting
   (`region.rs:712-720,1115-1138`). Like `svm_region_unmap_internal`, each
   detach releases the **current process's** mapping and uses the region's
   mapper accounting to decide whether the region itself can be removed; it
   does not wait for remote clients to unmap first. `memory_shared` does not
   individually free or destroy ring queues on every unmap. Unmap leaves
   `mapped_shmem_regions` empty but does not manage `private_rps` or reset
   `ring_misses`.
   Shared numeric fd fields and ring markers do not determine the region's
   mapper lifetime.

The queue's `#[repr(C)]` layout is naturally aligned, not
`#[repr(C, align(64))]`. On Linux x86_64 the expected *Rust* queue field
layout places the first element at offset 120 from the queue base; the
entire allocation still begins at a 64-byte boundary. Verify with Rust
`size_of`, `align_of`, `offset_of` for actual fields, and
`size_to_alloc = size_of::<SvmQueue>() + nels * elsize` with checked
arithmetic. The private message prefix places the payload at a Rust-computed
offset of 16 B on that target; its address-sized ring marker, length and GC
timestamp fit together without a second header object. `ShmemHeader` fixes
the hot-field order so `server_pid`, `input_queue`, and both `Vec<RingAlloc>`
metadata values occupy its first 64 B on that target. The remaining counters
can occupy another line; the header's total size is **not** declared equal
to VPP's. These are Hammer layout and cacheline budgets, not C ABI promises;
check them against the Rust fields and actual target rather than hand-calculating
or asserting C/Rust field equality. A
`SvmRegionHeader` remains cacheline-aligned by choice. Bump the enclosing
Hammer region/layout version and reject old mapped queues before attach;
there is no compatibility wrapper and no change to `SvmMsgQ` storage.

The short `SvmQueue` field queries and `can_send`, `ShmemHeader` PID/ring
queries, `ApiMain::shmem_header` and `MsgBuf::len` carry `#[inline]`;
`ApiMain::current` follows VPP's `always_inline vlibapi_get_main`
(`vlibapi/api_common.h:384-389`). The queue/guard ring-head slot
operations and `RingAlloc::alloc_slot` carry `#[inline(always)]` on their
signatures above. VPP marks its
internal ring allocation path `static inline` (`memory_shared.c:34-35`);
the per-class check is its corresponding hot path here. Keep lock/wait,
mapping, Data Heap fallback, encoding and heap release as ordinary
functions; do not blanket `#[inline(always)]`. Inlining the ring checks
trades instruction-cache size against call cost.
Only a later like-for-like Rust benchmark of cacheline touches, message
latency and CPU per message can establish the performance claim; no
benchmark result exists at design time.

## 5. Error contract

The mapping boundary alone needs `MapError`. It is owned by
`hammer-ipc::binary_api`; the caller can retry or stop the mapping.
Keep infra queue, region and codec errors with their existing owners.

| Branch | Result and recovery |
| --- | --- |
| Server backing open fails immediately | `MapError::Open { path, source }`; preserve the OS error and failed path, not a fabricated metadata error. |
| File unavailable for 100 s | `MapError::OpenTimeout { waited, source }`; retain the last OS open error as `#[source]`. |
| Header still unset for 100 s | `MapError::ReadyTimeout { waited }`; unmap the local attachment. |
| SVM create/attach fails | `MapError::Region { source }`; preserve the `SvmRegionError` chain. |
| Queue attachment fails validation | `MapError::Queue { source }`; preserve the original `SvmQueueError`; do not use the invalid queue. |
| Backing `fchown` fails | Report the original OS error as an advisory diagnostic and continue, matching VPP's warning on both new and already existing backing. The root/API backing owner, not the SVM region or message allocator, performs this step. |
| Invalid default queue geometry or override | `MapError::Queue { source }` preserves the underlying `SvmQueueError` before setting the shared header address. Custom `ShmElemConfig` is not accepted by ordinary mapping. |
| Ring head occupied | First observation sets GC seconds mark and tries next class; after `now - mark > 10` the **same** slot is forcibly reused and counted, even if the old peer still has its address. This is not an `Option` failure. |
| No current mapped header | ApiMain::alloc requires its mapping/header to be installed; missing setup violates its unsafe precondition. VPP's internal explicit-region allocator checks `user_ctx == NULL`, but its ordinary entry points already dereference `am->shmem_hdr` for pool selection. Do not invent a successful `None` result for a receiver that cannot exist. |
| Nullable Data Heap exhaustion | Only the `*_or_null` variants return `None` for heap exhaustion; non-nullable allocation has VPP's non-returning OOM behavior. No Main Heap fallback. |
| Invalid allocation length | Assert that the caller's payload length fits VPP's signed `int`, and check prefix/slot arithmetic before writing any shared length; neither silently truncate to `u32` nor report heap exhaustion. |
| Encode/decode failure | Existing `codec::Error`; an allocation not enqueued remains under the allocating caller's control. |
| Input queue full | Ordinary waiting `add` waits for space, exactly as `svm_queue_add(q, elem, 0)`; `can_send` does not reserve space. |
| Queue lock/signal failure | Existing generic `SvmQueueError` and its existing commit classification; the queue has copied an address only if the enqueue committed. No Binary API-specific rollback or automatic message release is invented. |
| Old mapped queue layout | Existing `SvmRegionError::UnsupportedVersion`, translated once through `MapError::Region`, before reading either shared header or queue. No new error for the shared-header version field. |
| Restart input mutex still busy after ten tries | Zero only that mutex's raw storage and proceed to drain as VPP does; this path is unsafe and has no `MapError` variant. Normal queue owner-death errors are unchanged outside restart. |
| API-region PID probe fails before restart mutation | `MapError::Region { source: SvmRegionError::ClientProbe { .. } }`; retain the OS source and do not enter restart mutation. VPP would instead remove the PID on any failed `kill`. |
| Root-region lock or PID probe fails after restart drain | The restarted region is already live. Emit an advisory diagnostic with the original `SvmRegionError` source and return map success, **not** an `Err` suggesting that the server restart was undone. A later root scan may clean stale PIDs. |
| Invalid shared pointer or drain error other than empty | Stop region use; do not read or free an uninitialized pointer-sized stack slot. |

Wrong server thread and a corrupted queue invariant after a validated attach are programmer
bugs, not ordinary `Option` outcomes. Mapping failures and timeouts stay
distinct; no catch-all transport error is introduced.

## 6. Approval and migration inventory

The repository owner's approval is recorded in Issue #308. The follow-up
correction opens the existing global accessor and adds the header borrow and
address conversion required to actually use allocated messages. The inventory
covers those corrected public APIs, not other entry points.
The existing types below cannot satisfy the desired shared ownership:
the separate shared header is not the queue object, and
`SvmQueueElements<T>` holds a local queue bundle and allocates on receive.

| Add/change/remove | Owner and why |
| --- | --- |
| Delete the separate shared header and local `SvmQueue` handle; replace both with shared `SvmQueue`; remove `SvmRegion` from queue init/attach, returning the actual shared queue address; add queue-bound guard, guarded/unlocked ring-head operations, restart-only unsafe mutex reset, advisory `can_send`, typed method-level operations; remove `SvmQueueElements<T>` | `hammer-infra::svm::queue`; VPP ring pointers and queue operations target one shared queue object without embedding the region in the generic queue; typed receive must not allocate a temporary `Vec`. Update exports and all callers of local queue construction/event-fd setup. `cleanup` destroys POSIX objects; the region heap owner separately frees the queue allocation only when retired, together matching `svm_queue_free`. |
| Keep `SvmQueueConfig`, `SvmQueueConditionalWait`, `SvmQueueElement` and actionable `SvmQueueError` categories; adjust lock/wait implementation and owner-death handling | `hammer-infra::svm::queue`; preserve existing queue behavior and error categories with a shared-only object. |
| Add `user_context`, `contains_range`, `set_user_context`; bump region/layout version | `hammer-infra::svm::region`; the existing region owns its `user_ctx`, mapping bounds and old-layout rejection. No second region owner. |
| Add `ShmemHeader`, private `RingAlloc`/`RingRole`, address-handle `MsgBuf`, borrowed `From<&MsgBuf> for usize`, and narrow `MapError` including immediate backing-open failure; defer `ShmElement`/`ShmElemConfig` to private-region work | `hammer-ipc::binary_api::memory_shared`; no existing Binary API message storage/ring owner. Prefix fields live in the allocation, with no separate header type. `MsgBuf` stores no Rust payload borrow, region or queue, and has no Drop; unsafe operation-scoped access acknowledges forced reclaim. Ordinary free uses the explicitly borrowed ApiMain that owns the originating mapping. The address conversion is a queue-slot value, not ownership transfer. Dequeue validation remains private implementation detail. |
| Extend existing `ApiMain` with current `rp`, stable `primary_rp`, private-region pointer list `private_rps`, ordinary owning list `mapped_shmem_regions`, current `shmem_header`, `process_pid`, `ring_misses`, input-queue length, API UID/GID, global base VA, root total/API data sizes, two independent PVT Heap sizes and API-region name. Add pre-installation configuration, UID/GID/base getters, `root_region_config`, `set_primary_region` after primary setup, one ordinary map and one ordinary-list unmap method; install the same `ApiMain` once without returning it into `api_global_main`, select through `my_api_main` per thread, then expose `current()`, `set_main()` and an unsafe current-header borrow | `hammer-ipc::binary_api::api`; VPP separates currently selected, primary, private memfd and ordinary mapped-region roles. Boxing each ordinary process-local `SvmRegion` keeps those pointers stable across vec growth; explicitly unmap every ordinary owner rather than merely dropping it. The root creator uses `global_base_va()` and the root-total config; root/API backing owners apply `api_uid()` and `api_gid()`; the API map passes API **data** size directly to `find_or_create_subregion`. `private_rps` stays empty until a separate private memfd owner is designed; it must not be populated by ordinary mapping or freed through its unmap method. ADR-0017 changes registry borrowing for post-install API-init; its server-only fields are documented there. No handler parameter or second main type is introduced. |
| Move PID scanning to the acquired `RegionLock`; retain `SvmRegion::remove_exited_clients` as a delegating entry | `hammer-infra::svm::region`; restart already owns the API region lock and must not reacquire it. Preserve the existing `ESRCH`/`EPERM`/typed `ClientProbe` distinction; root cleanup after restart is advisory. |
| Add direct uninitialized-output serialization; keep method-level `Api` generics on `MsgBuf::encode/decode` | `hammer-ipc::binary_api::codec` and `memory_shared`; current serializer takes initialized bytes, whereas nonzeroing allocation cannot expose an initialized mutable slice. |
| Add `hammer-infra` and `hammer-runtime` dependencies to `hammer-ipc` | `hammer-ipc/Cargo.toml`; consume existing SVM owners and the existing runtime main-thread check for the server ring, without another thread-identity field in `ApiMain`. |
| No foreign vector header, C probe, flexible-array Rust member, client-registration surface, or new handler API | Out of scope by explicit request. The one proposed global installation/accessor is for the **existing** `ApiMain` only; retain its codec and registration work without changing handler parameters. |

The generic queue migration includes its constructors, visibility, exports,
typed adapter users, queue unit/integration tests and any VPP-style queue
callers. ADR-0011's process-local queue examples are superseded by this
section; do not keep two contradictory implementations. This ADR does not
claim that code or performance has already been changed.

## 7. Verification and verdict

Do **not** write or run `memory_shared` tests while that subsystem is
unfinished. At the eventual implementation's final pre-commit gate, run
only the affected `hammer-infra` SVM queue/region tests, formatting check
and `git diff --check`; no test command during implementation. Derive
SVM tests from VPP behavior, not from source-text matching:

| SVM-only case | Observable assertion | Decision |
| --- | --- | --- |
| Aligned allocation and inline elements | Rust queue base is 64-byte aligned; first element is immediately after the naturally aligned queue fields; checked allocation covers all slots. No C probe. | Shared queue and layout |
| Shared attach and old-layout rejection | Same-build mapping borrows the actual shared queue; the existing `SvmRegion` version check rejects old layout before queue access; invalid queue size or out-of-range allocation is rejected before reading elements. No extra shared-header-version error. | Mapping lifetime |
| Add/sub/wait/owner death | Exclusion, wakeup, timeout and typed robust owner-death errors retain current behavior; full queue stays unchanged on nowait failure. | Lock and errors |
| Restart-only mutex recovery | With no live queue users, a successful trylock is immediately unlocked; an inaccessible mutex is attempted ten times at 10 ms intervals before raw reset. Ordinary lock recovery remains unchanged. | Queue restart primitive |
| Ring-head access on a generic queue | Guarded client advance and exclusive server-main-thread advance inspect the same inline slots without changing add/sub occupancy. | Ring ownership |
| Pointer-sized queue element | `add`/`sub` copy exactly `size_of::<usize>()` bytes into/out of one inline slot; the original message allocation is not copied by queue operations. | Queue data movement |
| Typed dequeue | Runtime size mismatch reports the existing concrete error; successful dequeue needs no heap allocation. | Typed methods |
| Generic region PID scan | Under one acquired `RegionLock`, remove a departed subprocess PID, preserve a living client's PID, and leave the existing public scan result unchanged; root and API regions use the same method. EPERM retention and other OS probe errors remain the existing `process_is_dead` contract, not a new VPP-style remove-on-any-error policy. | Locked scan and intentional PID-probe difference |

Performance acceptance comes **after** the whole transport is implemented:
compare equivalent message sizes and client/server contention on the same
target, inspecting cacheline footprint and CPU/message. No C ABI fixture
or C probe is part of that acceptance.

Design verdict: **Approved for implementation**, not yet verified in code.
The ten-second reclaim and forced restart mutex reset now follow VPP's actual
branches; neither is safe in the presence of a live peer accessing reclaimed
storage or the reset mutex. In particular, zeroing a process-shared robust
POSIX mutex does **not** reinitialize its process-shared/robust attributes;
the resulting object's cross-process behavior is implementation-dependent.
That risk is part of the requested VPP behavior, not a safe-Rust guarantee.
The remaining open requirement is measured CPU equivalence after the entire
transport is implemented, not a missing queue ownership decision.

### Implementation review (2026-09-14)

Scope: generic `hammer-infra::svm` queue/region and the ordinary
`hammer-ipc::binary_api` shared-memory path. VPP counterparts are
`svm/queue.{c,h}`, `vlibapi/memory_shared.{c,h}`, `vlibapi/api_shared.c`,
`vlibapi/api_common.h`, and the client main-selection calls in
`vlibmemory/memory_client.c` and `vcl/vcl_bapi.c`.

Historical ordinary-path review; **not a completion verdict for ADR-0017**.
TLS/client ownership and allocation/free signatures below incorporate its
revision; client generation, lifecycle integration and behavioral checks remain
open as recorded there. The shared `SvmQueue` contains its inline elements at offset 120
from a 64-byte-aligned allocation; the ring retains a pointer to that queue,
tries larger classes and reclaims after more than ten seconds. Ordinary
allocation uses the actual `&ApiMain` and compares its PID with the shared
server PID; `current()` uses the default `api_global_main` unless `my_api_main`
selects a different instance. Async clients retain their own Main reference. Client rings lock their queues, server rings use the
main thread, and fallback and release activate the mapped region's Data Heap.
The input queue accepts only a pointer-sized address through its waiting
`add`; restart resets the input mutex after ten failed trylocks and drains
under the acquired region lock. The typed server backing-open failure retains
its OS source instead of claiming a metadata failure.

Non-blocking differences: the shared ring vectors are Rust same-build values,
not VPP vecs, and the `ShmemHeader` has a larger cacheline footprint; no C
interoperability is promised. A selected non-default `ApiMain` must be
`'static`, and its client worker's mapping lifecycle remains outside this
ordinary-region task. `SvmQueue` event-fd numbers require corresponding
local descriptors in every participating process. Corrupted shared `Vec`
metadata cannot be safely inspected, so attach's unsafe precondition requires
a same-build initialized header rather than accepting arbitrary shared bytes.
CPU/cacheline parity is still unmeasured; no `memory_shared` tests are added
at the owner's request. Compile checks run: `cargo check -p hammer-infra
-p hammer-ipc` and `cargo check -p hammer-ipc`. The SVM-only test gate is
reserved for the final pre-commit step.

## 8. Implementation tasks

These tasks are ordered by dependency. Completing a task means its stated
behavior and ownership contract is in code; checking a later task does not
waive an earlier one. Section 3 specifies the proposed signatures, section 5
the error branches, and section 7 the **SVM-only** verification gate.

1. [x] **Approve the proposed API surface.** The repository owner approved
   section 6, including the shared `SvmQueue`, existing `ApiMain` global
   installation and the unsafe boundaries, in Issue #308.
2. [ ] **Migrate `hammer-infra::svm::queue`.** Make `SvmQueue` the shared object
   at a 64-byte-aligned allocation base with inline elements; remove the local
   `SvmQueueHeader`/`SvmQueueElements<T>` model. Preserve existing queue
   error/owner-death behavior and event notification, add queue-bound guards,
   pointer-sized operations and method-level typed dequeue without a temporary
   heap allocation. Done when existing callers compile against the new owner
   and the SVM-only queue cases in section 7 pass.
3. [ ] **Extend `hammer-infra::svm::region`.** Add acquired-lock PID scanning,
   `user_context` publication/read and mapped-range checking; make the
   existing region scan delegate to its acquired guard. Bump the generic
   region version to reject previous queue layout. Done when the region's
   existing scan behavior and the SVM-only region cases in section 7 pass,
   including the `ESRCH`/`EPERM`/`ClientProbe` distinction.
4. [ ] **Extend the existing Binary API codec and `ApiMain`.** Serialize
   directly into `MaybeUninit<u8>` without intermediate payload storage while
   retaining the current initialized-slice codec API. Install the existing
   `ApiMain` once; add the distinct `rp`, `primary_rp`, `private_rps` and
   `mapped_shmem_regions` roles, current header, PID/counters and all root/API
   size, PVT, fixed-VA, UID/GID and name configuration from section 3. Done
   when root setup uses `global_base_va()` and `root_region_config()`, both
   backing owners apply `api_uid()`/`api_gid()`, and the ordinary API map uses
   `api_size` as **data bytes**, not the entire mapped length.
5. [ ] **Implement shared header, rings and `MsgBuf` in `hammer-ipc`.** Use the
   existing SVM Data Heap and queue, fixed-VA ring vectors and a private
   prefix without a zero-length Rust array or a separate header type. Enforce
   signed allocation length, checked prefix/slot arithmetic, PID-selected
   server/client rings, client queue mutex and server main-thread ownership;
   miss into larger rings, force reuse only after ten seconds, then fall back
   to the originating region's Data Heap. Done when message encode/decode use
   method-level `T: Api` and heap release switches/restores the originating
   Data Heap without binding `MsgBuf` to a queue.
6. [ ] **Implement ordinary map, pointer handoff, restart and unmap.** Set
   `user_ctx` only after initialization and restoring the previous heap;
   attach through the existing region/queue validation. Enqueue the payload
   **address** with waiting `SvmQueue::add`, not a copied payload or a new
   `MsgBuf` send method. On restart scan the API region under its held lock,
   try the input mutex ten times before forced reset, drain/free old addresses
   without relocking the region, then scan the root after unlocking; root
   cleanup errors are advisory after restart has completed. Ordinary unmap
   consumes only `mapped_shmem_regions` through `SvmRegion::unmap`, leaving
   private memfd lifetime to its separate owner. Done when all branches in
   section 5 retain their stated error and ownership semantics.
7. [ ] **Run the final infrastructure verification gate.** After the
   implementation and review are ready to commit, run only the affected
   `hammer-infra` SVM queue/region tests, formatting check and diff check,
   then commit if they pass. Do not add or run `memory_shared` tests while the
   transport is unfinished; do not add a C probe or claim CPU equivalence
   before a later same-target benchmark. Do not introduce handler business
   logic, client registration, foreign layouts or additional main types.
