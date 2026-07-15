# VPP-Style Memory Management Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build Hammer's VPP-style memory model with a concrete explicit heap handle, inline buffer storage, NUMA-selected buffer pools, worker-local buffer caches, and frame ownership that drops resources automatically on the data-plane hot path.

**Architecture:** `hammer-infra::heap::Heap` is a concrete opaque handle (private vtable + retained backend state), constructed with `Heap::main()` / `Heap::local()` for Hammer's approved mature `mimalloc::MiMalloc` main-heap backend or `Heap::svm_data(region)` for owner-created shared-memory data regions, then passed around as `Arc<Heap>`. The private vtable uses Rust `GlobalAlloc`-shaped raw pointer callbacks (`alloc -> *mut u8`, `dealloc(ptr, layout)`) only to guarantee same-handle deallocation; this mirrors local VPP's opaque active-heap pointer model (`clib_mem_thread_main.active_heap`, `clib_mem_get_heap`, `clib_mem_set_heap`) without pretending that heaps are NUMA registries or porting VPP's dlmalloc mspace implementation. SVM support in this slice adapts only the owner-side data-heap subset: the owner mapping is `memfd`/`shm_open`/`mmap` backed and claimed by `talc::TalcLock<spinning_top::RawSpinlock, talc::source::Manual>`; region-heap/private-heap split and attach-side fixed-VA allocation remain out of scope. Memory initialization is registered through Hammer's existing static `linkme` init-function slices and materializes fixed buffer-pool tables before workers start; there is no runtime `Mutex`, `RwLock`, `OnceLock`, lazy global map, or hot-path lookup lock. `hammer-adapter::buffer::BufferPoolArena` owns one heap allocation, lays out each slot as `[Buffer header | 128 bytes pre-data | inline data]`, and resolves `BufferIndex.slot()` with `base + slot * stride`, Hammer's generational adaptation of VPP's buffer-memory index math. Data-plane ownership moves through Hammer RAII values `Frame<Next>` and `Frame<Pending>`; these are Rust owner wrappers around next/pending frame contents, not VPP ABI objects, and `Drop` owns packet/frame cleanup so callers outside `buffer.rs` get no lifetime function to call.

**Tech Stack:** Rust 2024, `std::alloc::{GlobalAlloc, Layout}`, `mimalloc::MiMalloc`, `talc::{TalcLock, source::Manual}`, `spinning_top::RawSpinlock`, `std::ptr::NonNull`, `Arc<Heap>`, `libc::{memfd_create, shm_open, ftruncate, mmap, munmap, close}`, existing `SvmRegion`, existing `BufferThreadCache`, existing `FramePool`, targeted `cargo test -p ...`, `cargo fmt --all -- --check`, targeted `cargo clippy -p ... --all-targets`.

## Global Constraints

- Do not model Heap as a Rust trait, a generic `H: Heap`, or a trait object. The only public heap handle is the concrete `hammer_infra::heap::Heap` shared as `Arc<Heap>`.
- Do not use `dyn GlobalAlloc`, `dyn Allocator`, or `#[global_allocator]` for this work. `GlobalAlloc` is only the callback shape for the private `HeapVTable`; Hammer containers still retain an explicit `Arc<Heap>` and deallocate through the same handle.
- Do not introduce `HeapLocal` or `HeapSvm` public types. Local and SVM allocation are constructors on the concrete handle: `Heap::main()`, `Heap::local()`, and `Heap::svm_data(SvmRegion)`. NUMA selection belongs to buffer-pool/runtime configuration, not the heap handle.
- Do not hand-roll Hammer's main heap allocator. Hammer's main heap uses `mimalloc::MiMalloc`, a mature allocator that implements `GlobalAlloc`; this is a Hammer backend decision, while local VPP's main heap is `clib_mem_heap_t` backed by dlmalloc mspaces. Do not use `std::alloc::System` as the planned main-heap backend, do not port VPP's `mem_dlmalloc.c` or `heap.h` object-heap utilities into Hammer for this plan, and do not add `jemalloc`/`snmalloc`/`rpmalloc` without a separate approval item.
- Do not use `std::vec::Vec`, `hammer_infra::vec::Vec`, or any Vec-backed helper for allocator metadata, NUMA buffer-pool registry state, buffer-pool metadata, or memory-initialization tables. Use fixed arrays, raw pointer allocations, or mature allocator-owned metadata. Do not hand-roll returned-span lists.
- `hammer-infra` stays the bottom layer. It may expose generic allocation primitives, but it must not know about buffers, nodes, runtime, sessions, or transport.
- Buffer/runtime APIs remain transport-neutral. Do not add TCP-specific allocation, headroom, copy, or rebuild helpers.
- Buffer slots follow the VPP semantic shape from local `third_party/vpp/src/vlib/buffer.h`: signed `current_data: i16`, `NEXT_PRESENT` + `next_buffer` chain links, `ref_count: u8`, `buffer_pool_index: u8`, inline `pre_data`, inline `data[]`, per-worker cache allocation/return, and slot-to-pointer math through a pool-owned region.
- The app/session/TCP boundary rules from AGENTS.md still apply: no payload-copy side channels after bytes enter session-owned storage, and no TCP-specific buffer ownership wrappers.
- Automatic cleanup means runtime/service/node hot paths never call packet/frame lifetime functions directly. Packet/frame cleanup is triggered by owner `Drop`; the only low-level ownership hook stays private to `buffer.rs` and is called only from `Drop`.
- Memory initialization must be static and no-lock: use `linkme` init-function registration plus fixed arrays/engine-owned tables. Do not use `Mutex`, `RwLock`, `OnceLock`, `LazyLock`, `get_or_init`, `thread_local!`, hash maps, or global mutable registries for heap/buffer initialization or NUMA buffer-pool lookup.
- Do not keep compatibility parameters by naming them `_`, `_node`, `_next`, or similar. If a migration makes an argument obsolete, delete the argument from the function signature and update every caller in the same task.
- Use the existing `hammer-infra` structures before adding new ones. Public API additions listed in this plan are the only approved additions for this memory-management slice.
- TDD rhythm per task: write/adjust the named failing test, run it red, implement the minimum code, run it green, then commit.
- All VPP references in this plan are from local source under `third_party/vpp`. Do not substitute remote VPP links unless the local tree is missing the needed file.
- Before implementing any heap, SVM, buffer, NUMA, or cache task, re-read the specific `third_party/vpp` files named in the File Map and keep Hammer's design a semantic Rust adaptation of those local sources.

---

## File Map

- `third_party/vpp/src/vppinfra/mem.h`: active heap state (`clib_mem_thread_main.active_heap`) and `clib_mem_get_heap` / `clib_mem_set_heap`.
- `third_party/vpp/src/vppinfra/mem_dlmalloc.c`: VPP `clib_mem_heap_t` implementation over dlmalloc mspaces; this is the local VPP main-heap reference and is not being ported.
- `third_party/vpp/src/vppinfra/heap.h`: VPP object/vector heap utility; this is not the `clib_mem_heap_t` main heap and is not being ported.
- `third_party/vpp/src/svm/{svm.h,ssvm.h}`: SVM heap push/pop helpers that temporarily switch the active heap to shared-memory regions.
- `third_party/vpp/src/svm/svm.c`: `MAP_FIXED` attach flow plus region/data heap creation with `clib_mem_create_heap(...)`.
- `third_party/vpp/src/svm/ssvm.c`: fixed-VA attach behavior used to preserve shared heap pointer validity across processes.
- `third_party/vpp/src/vlib/buffer.h`: `vlib_buffer_t`, `VLIB_BUFFER_PRE_DATA_SIZE`, `VLIB_BUFFER_POOL_PER_THREAD_CACHE_SZ`, `vlib_buffer_pool_t`, and `vlib_buffer_main_t`.
- `third_party/vpp/src/vlib/buffer_funcs.h`: `vlib_get_buffer`, `vlib_buffer_alloc_from_pool`, `vlib_buffer_alloc_on_numa`, `vlib_buffer_pool_put`, and the C buffer-return path that Hammer wraps in Rust ownership.
- `third_party/vpp/src/vlib/buffer.c`: `vlib_buffer_alloc_size`, `vlib_buffer_pool_create`, NUMA pool creation, and `default_buffer_pool_index_for_numa` initialization.
- `third_party/vpp/src/vlib/main.h`: `vlib_main_t.thread_index` and `vlib_main_t.numa_node`.
- `third_party/vpp/src/vlib/{init.h,init.c}`: static init registration and topologically ordered init/worker-init dispatch.
- `third_party/vpp/src/vlib/threads.c`: worker `vlib_main_t` cloning, per-thread heap selection, and worker `thread_index` assignment.
- `Cargo.toml`: workspace dependency entries for `mimalloc`, `talc`, and `spinning_top`.
- `crates/hammer-infra/Cargo.toml`: direct allocator dependency.
- `crates/hammer-infra/src/heap.rs`: concrete heap handle, private hand-written vtable, local/SVM constructors.
- `crates/hammer-infra/src/svm_region.rs`: reusable mmap/memfd region with a Talc-backed owner allocator over the mapped span.
- `crates/hammer-infra/src/{pool.rs,vec.rs,boxed.rs,map.rs}`: infra containers that retain `Arc<Heap>` and deallocate through the same handle.
- `crates/hammer-runtime/src/memory.rs`: static no-lock memory initialization; any fixed NUMA table stays private to runtime/buffer internals and is not a public API.
- `crates/hammer-adapter/src/buffer.rs`: inline buffer slot layout, buffer/frame pools, worker cache behavior, RAII frame ownership.
- `crates/hammer-runtime/src/{init.rs,memory.rs}`: existing static init dispatch plus memory init function registered before `start_workers`.
- `crates/hammer-adapter/src/{node.rs,node/next.rs,handoff.rs}`: packet graph ownership transfer sites that must carry frame owners instead of raw lifetime calls.
- `crates/hammer-runtime/src/{engine.rs,start_workers.rs,spawn.rs}`: worker NUMA propagation and runtime-owned frame handoff call sites.
- `crates/hammer-service/src/**`: service nodes that currently drop packets through direct lifetime calls and must switch to frame ownership/drop semantics.
- `crates/hammer-infra/tests/{heap.rs,pool_heap.rs,slice_map_heap.rs}`: concrete heap and raw infra allocation tests.
- `crates/hammer-adapter/tests/{buffer_inline_layout.rs,buffer_frame_guard.rs,buffer_per_numa.rs}`: buffer storage, RAII, and NUMA tests.
- `crates/hammer-service/tests/frame_owner_cleanup_hot_path.rs`: source-level invariant that runtime/service/node hot paths rely on owner `Drop`.

## Public API Approval Ledger

Approved/required existing or new public APIs:

- `pub struct Heap` in `hammer-infra`, with `Heap::main`, `Heap::local`, `Heap::svm_data`, `Heap::alloc`, `Heap::is_main_heap`, `Heap::region`, and `unsafe impl GlobalAlloc for Heap`. `HeapVTable` is a private implementation detail, not a public API. `Heap` does not expose a NUMA identity.
- `pub enum HeapError` and `pub enum SvmRegionError` in `hammer-infra` for allocator construction and mmap/fd failures. Do not report allocator setup failures through ad hoc strings or panics in runtime initialization paths.
- `pub struct SvmRegion` with `try_with_size`, `from_fd`, `from_fd_owned`, `base`, `size`, `fd`, and `alloc`. SVM deallocation is not a public API; it is reached only by the private `Heap` vtable's `dealloc` callback.
- `Pool::with_capacity_in(capacity, Arc<Heap>)`, `Slice::from_elem_in(len, value, Arc<Heap>)`, and `FlatHashTable::with_capacity_in(capacity, Arc<Heap>)`.
- `BufferPoolArena::with_capacity_in(slot_capacity, slots, Arc<Heap>, numa_node: u32)` and the existing back-compat `with_capacity(slot_capacity, slots)` local-heap constructor.
- `BufferPool::{slot_stride, base_ptr, buffer_raw_ptr, data_raw_ptr}` for layout tests and diagnostics.
- `buffer_data_offset() -> usize`, `DEFAULT_PRE_DATA_SIZE: usize = 128`, `BUFFER_THREAD_CACHE_BATCH: usize = 32`, and `BUFFER_THREAD_CACHE_HIGH_WATER: usize = 512`.
- `pub(crate) const HAMMER_MAX_NUMA_NODES: usize = 32` if a shared const is required, matching local VPP's `VLIB_BUFFER_MAX_NUMA_NODES`. Do not expose a public NUMA table or memory manager type.
- `DataPlaneBufferConfig` and `DataPlaneRuntimeConfig` as the single construction surface for `DataPlaneBuffers` / `DataPlaneRuntime`; all old `with_*` constructors are removed.
- `hammer_runtime::memory::memory_init(engine: &mut Engine) -> HammerResult<()>`, registered as `#[init_function(name = "memory_init", runs_before = ["start_workers"])]`.
- `DataPlaneRuntime::for_worker(thread_index: u32, numa_node: u32) -> Self`, used by `Engine::spawn_on_numa`.
- `Frame<Next>` and `Frame<Pending>` remain the public typed owner shapes. `Next` and `Pending` are concrete state bodies that directly own the frame fields; they are not marker types, do not use `PhantomData`, and do not delegate ownership to `FrameStorage` or `FrameOwner`.
- `Frame<Next>` as Hammer's public RAII owner for next-frame contents already associated with a selected next arc, and `Frame<Pending>` as Hammer's scheduler-owned pending-frame RAII owner while a node is dispatching it. These are Rust ownership wrappers, not direct public equivalents of `vlib_next_frame_t` or `vlib_pending_frame_t`.
- `DataPlaneBuffers::get_next_frame(next: NodeId) -> CoreResult<Frame<Next>>` and `DataPlaneRuntime::put_next_frame(frame: Frame<Next>) -> CoreResult<()>`, using VPP-inspired get/put terminology while keeping Hammer's existing `NodeId` graph model. There is no public pending-queue extraction API and no new pending-process callback type in this plan.
- No public `Frame` state-conversion traits or helper methods are approved. `DataPlaneRuntime::put_next_frame` consumes `Frame<Next>` and constructs the scheduler-owned `Frame<Pending>` inside `buffer.rs` without exposing `FrameIndex`. There is no public `Frame<Pending> -> Frame<Next>` transition; a node that needs a fresh next-frame owner calls `DataPlaneBuffers::get_next_frame(next)`.

Rejected APIs for this plan:

- `pub trait Heap`, `HeapLocal`, `HeapSvm`, generic public `H: Heap` constructors, or any heap surface that requires dynamic dispatch through Rust trait objects.
- Hand-written main-heap bin/slab/free-list allocators in Hammer, ports of VPP `heap.h` internals for local memory, or defaulting to an allocator crate through `#[global_allocator]`.
- TCP-specific buffer allocation/lifetime helpers, TCP-specific headroom APIs, and single-buffer owner wrappers.
- Public constructors for recovery/buffer accounting records that let non-owning code manufacture ownership.
- Any initialization API that hides heap/buffer state behind `Mutex`, `RwLock`, `OnceLock`, `LazyLock`, `get_or_init`, `thread_local!`, or dynamic global maps.
- Public manager/table/helper types such as `HeapRegistry`, `MemoryConfig`, `MemoryMain`, `StaticNumaTable`, pending-process callback aliases, `ScheduledFrameQueue`, `HandoffSlotGuard`, or `SegmentAllocOwner`. If a fixed table or guard is unavoidable, keep it private inside the owning module and do not expose it through `lib.rs`.
- Public or private frame-owner helper types/traits such as `FrameStorage`, `FrameOwner`, or `FrameStateAccess`. `Frame<S>` plus concrete `Next` / `Pending` state bodies are enough.
- Public packet/frame APIs named `free_*`, `release_*`, `reclaim_*`, or public region APIs named `free`; ownership must move into `Drop` or allocator `GlobalAlloc::dealloc` implementations instead.
- Treating `SvmRegion::from_fd` as an allocation-owner heap. VPP supports attach-side `ssvm_mem_alloc` through a shared fixed-VA heap; this Hammer slice does not. If Hammer needs attach-side allocation, stop and write a separate VPP-compatible shared-heap design instead of extending this owner-only allocator.
- Public `Frame<Next>` transfer helpers such as `append_to_frame` or `enqueue_to_next_frames`. Frame movement must use the approved Hammer RAII next-frame surfaces: `DataPlaneBuffers::get_next_frame(next)`, current-frame ownership during dispatch, and `DataPlaneRuntime::put_next_frame(frame)`.
- Any public or private graph API named `submit_frame`; Hammer uses `put_next_frame` terminology for next-frame ownership.
- `NodeNextFrames`, `NextFrame`, `NodeNextFrame`, `Current(NodeId)`, `NodeResult::next_frame`, `NodeResult::next_current`, or any equivalent staging/result-carrier API that returns next-frame ownership from a node. VPP nodes fill and put Next Frames while they run; dispatch results do not carry frames back to the scheduler.
- `NodeResult::error`, `NodeResult::error(CoreError)`, or any equivalent attempt to turn graph dispatch outcomes into error propagation. If `NodeResult` remains in Hammer during this slice, it is only a dispatch outcome and must not carry frame ownership; broader node-error propagation requires a separate approved node-runtime plan.
- Persisting `Frame<Pending>` outside scheduler-owned node dispatch, or making `Frame<Pending>` `Send` for TUN/background ownership, without a separate approved design.

---

## Local VPP Source Findings

- Active heap: `third_party/vpp/src/vppinfra/mem.h` stores the current allocator as `clib_mem_thread_main.active_heap`; `clib_mem_get_heap()` lazily initializes it, and `clib_mem_set_heap(heap)` swaps the raw heap pointer and returns the previous one. Hammer therefore uses a concrete `Heap` handle with a vtable + payload pointer, not a Rust trait-object heap.
- VPP main heap: `third_party/vpp/src/vppinfra/mem_dlmalloc.c` implements `clib_mem_heap_t` over dlmalloc mspaces. Hammer does not port that allocator; `mimalloc::MiMalloc` is the approved Hammer main-heap backend behind the same explicit `Heap` handle model.
- VPP object heap utility: `third_party/vpp/src/vppinfra/heap.h` is an offset/handle object heap utility, not the `clib_mem_heap_t` main heap. It is not part of this Hammer main-heap plan.
- Worker main heap: `third_party/vpp/src/vlib/threads.c` captures `clib_mem_get_heap()` as `main_heap`, optionally creates a worker `thread_mheap` with `clib_mem_create_heap`, and temporarily installs it through `clib_mem_set_heap` while cloning worker state. Hammer's adaptation is explicit `Arc<Heap>` retention by each owner that allocates memory; NUMA selection happens in buffer pools, not in heap identity.
- SVM heap switching: `third_party/vpp/src/svm/svm.h` and `third_party/vpp/src/svm/ssvm.h` allocate inside shared-memory regions by saving the old heap, calling `clib_mem_set_heap(region_heap_or_data_heap)`, allocating, and restoring the old heap. Local `third_party/vpp/src/svm/svm.c` creates separate SVM region/private heap and data heap over mmap memory with `clib_mem_create_heap(...)`, and `svm.c` / `ssvm.c` remap attachers at fixed virtual addresses with `MAP_FIXED` so shared heap pointers remain valid. Hammer's current slice deliberately narrows this to the owner-created data-heap subset: `Heap::svm_data(SvmRegion)` is owner-only, `SvmRegion::from_fd` is offset access to already-laid-out objects, Hammer attach mappings do not preserve shared allocator pointer validity through a fixed-VA contract, and full attach-side allocation or separate SVM region-heap modeling is a separate unapproved design.
- Buffer header/data layout: `third_party/vpp/src/vlib/buffer.h` defines `vlib_buffer_t` as a two-cacheline header plus cacheline-aligned headroom, `pre_data[VLIB_BUFFER_PRE_DATA_SIZE]`, and flexible `data[]`; it asserts pre-data is directly before packet data. Hammer's layout must keep `Buffer` as 128 bytes, then 128 bytes pre-data, then inline data.
- Buffer current pointer: `vlib_buffer_get_current(b)` returns `b->data + b->current_data` and asserts `current_data >= -VLIB_BUFFER_PRE_DATA_SIZE`. Hammer's `Buffer::current_ptr()` must use exactly that signed-offset model.
- Buffer pool identity: `vlib_buffer_pool_t` stores `index`, `numa_node`, `data_size`, `alloc_size`, `n_buffers`, `n_avail`, `buffers`, per-thread `threads`, and a `buffer_template`. Hammer keeps `pool_id + generation` for Rust stale-index safety, but the pool's slot metadata must stay separate from packet data.
- Per-thread cache: `VLIB_BUFFER_POOL_PER_THREAD_CACHE_SZ` is 512. `vlib_buffer_alloc_from_pool` first consumes `bp->threads[vm->thread_index].cached_buffers`; on cache miss it refills from the pool availability vector, rounded in chunks of 32. `vlib_buffer_pool_put` fills the per-thread cache to 512 before spilling overflow back under the pool lock. Hammer's `BufferThreadCache` should use the same high-water size and refill/spill behavior.
- NUMA selection: `vlib_buffer_alloc(vm, ...)` calls `vlib_buffer_alloc_on_numa(vm, ..., vm->numa_node)`, which uses `default_buffer_pool_index_for_numa[numa_node]` and then `vlib_buffer_alloc_from_pool`. Hammer worker clones must select the active buffer pool from `Engine.numa_node`.
- Buffer pool creation: `vlib_buffer_pool_create` walks a single mapped memory range, computes `alloc_size = round_pow2(ext_hdr + sizeof(vlib_buffer_t) + data_size, VLIB_BUFFER_ALIGN)`, wastes at most one buffer only when needed so global buffer index 0 is never valid, writes `buffer_template`, and records buffer indices in `bp->buffers`. Hammer does not need VPP's global cacheline-index encoding, but it must preserve O(1) slot-to-pointer math and avoid per-buffer payload allocations. Do not describe this as a rule that every pool's slot 0 is reserved.
- Next-frame dispatch: `third_party/vpp/src/vlib/node_funcs.h` defines `vlib_get_next_frame` / `vlib_put_next_frame`; `third_party/vpp/src/vlib/main.c::vlib_put_next_frame` turns a non-empty next frame into a `vlib_pending_frame_t`, and `dispatch_node` calls the node function for its processed-vector count. VPP's `vlib_next_frame_t` is a next-arc enqueue state tied to the current node runtime, and `vlib_pending_frame_t` is a scheduler queue entry; Hammer's `Frame<Next>` / `Frame<Pending>` are Rust RAII owner wrappers inspired by those states, not a direct public VPP state-machine clone. VPP nodes do not return next frames through a dispatch result, so Hammer must not model forwarding as `NodeResult::next_frame`, `NextFrame`, or `Current(NodeId)`.
- Static initialization: `third_party/vpp/src/vlib/init.h` registers init functions through static constructor-linked records, and `third_party/vpp/src/vlib/init.c` sorts and dispatches them before workers run. Hammer already has the Rust equivalent in `hammer-runtime::init` through static `linkme` slices; memory init must use that path, not lazy globals.
- Explicit VPP buffer-return path vs Rust RAII: VPP uses explicit C buffer put functions and drop nodes. Hammer's automatic-cleanup rule is a Rust ownership adaptation: packet/frame cleanup is owned by `Drop`, while runtime/service/node hot paths transfer or drop frame owners and never call direct lifetime helpers.

---

## Grill Session Scope Correction

This section is binding for the current execution pass. It corrects the scope drift found during the grill-with-docs review and must be read before any implementation task.

### Domain Model

- `Heap`: Hammer's explicit allocator handle. It is a concrete public type, like VPP's opaque active heap pointer, and is never a Rust trait object. It does not carry public NUMA identity.
- `HeapVTable`: a private implementation table inside `heap.rs`. It exists to route allocation/deallocation through the same concrete handle and must not appear in the public API ledger.
- `Main heap`: Hammer's mature local allocator backend behind `Heap::main` / `Heap::local`. The approved backend is `mimalloc::MiMalloc`; this is not a port of VPP's dlmalloc mspace main heap and is not modeled as a per-NUMA heap table.
- `SVM owner data region`: the process-created shared-memory mapping that owns the Talc allocator over its mapped bytes. This corresponds only to the owner-side SVM data-heap subset in this plan.
- `SVM attached region`: a mapping of an existing fd used for offset access to already-laid-out objects. It is not an allocation owner in this plan because Hammer does not implement VPP's fixed-VA shared heap pointer-validity contract.
- `BufferPoolArena`: one heap allocation containing all buffer slots for one buffer pool. A slot is `[Buffer header | pre_data | inline data]`; the pool records its NUMA node for selection by worker runtime.
- `BufferIndex`: a generational index into a pool-owned slot. It is not an owner by itself.
- `Frame<Next>`: Hammer's RAII owner for a next-frame payload already associated with one selected next arc. It is inspired by VPP next-frame get/put semantics, but it is not a public `vlib_next_frame_t` clone and it must not require an extra `NodeId` parameter when submitted.
- `Frame<Pending>`: Hammer's scheduler-owned pending-frame RAII owner delivered to node dispatch. It must not be persisted outside dispatch or made `Send` in this plan.
- `NodeResult`: graph-node dispatch outcome. It is not an error transport and not a carrier for Next Frames.

### ADR Notes

**ADR-1: Concrete heap handle, private vtable**

Decision: expose only `pub struct Heap`; keep `HeapVTable` private.

Reason: local VPP exposes opaque heap pointers and active-heap setters, not public allocator callback tables. Rust still needs same-handle deallocation, so Hammer uses a private vtable rather than `dyn Heap`, `dyn GlobalAlloc`, or generic `H: Heap`.

Consequences:

- `Heap::alloc` is the public allocation convenience.
- Deallocation remains available through `unsafe impl GlobalAlloc for Heap` for owners that retained the handle.
- No public `Heap::dealloc`, `HeapVTable`, `HeapLocal`, or `HeapSvm`.

**ADR-2: Mature Hammer main heap, not VPP allocator port**

Decision: use `mimalloc::MiMalloc` behind `Heap::main` / `Heap::local`.

Reason: local VPP's main heap is `clib_mem_heap_t` over dlmalloc mspaces. Porting that allocator into Rust would be a separate allocator project and is not required to get Hammer's VPP-style explicit heap ownership. The semantic requirement here is explicit heap handles and same-handle deallocation, not byte-for-byte VPP allocator internals.

Consequences:

- Do not hand-roll bins, slabs, returned-span lists, or a local bump/free-list allocator for the main heap.
- Do not use `std::alloc::System` or `#[global_allocator]`.
- Do not describe `mimalloc` as VPP's Rust equivalent; describe it as Hammer's approved backend.

**ADR-3: Owner-only SVM allocation**

Decision: `Heap::svm_data(SvmRegion)` is valid only for the allocation-owner data mapping. `SvmRegion::from_fd` / `from_fd_owned` create attach mappings for offset access only.

Reason: VPP attachers allocate from shared heaps only after fixed-VA remapping preserves allocator pointer validity. Hammer does not implement that fixed-VA shared heap contract in this slice.

Consequences:

- Full attach-side allocation requires a separate VPP-compatible shared-heap design.
- Do not add top-level helpers named `svm_alloc`, `svm_dealloc`, or `alloc_region`.
- Do not create hand-written SVM returned-span metadata.

**ADR-4: Rust RAII adapts VPP explicit buffer return**

Decision: runtime/service/node hot paths transfer frame ownership and let `Drop` return packets/frames.

Reason: VPP C code explicitly calls buffer put/free paths. Hammer can encode the same ownership through Rust values, avoiding caller-side lifetime functions and double-owner bugs.

Consequences:

- Public packet/frame lifetime functions are rejected.
- Public frame-transfer shortcuts are rejected unless separately approved.
- The scheduler owns `Frame<Pending>` until dispatch; service code may not squirrel it away.

### Current Tree Problems Found By Grill

- **Resolved in cleanup pass:** `crates/hammer-adapter/src/node.rs` must not expose any pending-process callback alias or descriptor pending-process plumbing as public API. Pending frames are dispatched through the normal node `process(&mut BufferFrame)` path.
- **Out of plan:** `crates/hammer-adapter/src/memory.rs` still exposes `MemoryConfig`, `MemoryMain`, and `StaticNumaTable`, and `DataPlaneBufferConfig` exposes the static NUMA table through a public field. Fixed tables must be private implementation details behind the config construction surface.
- **Out of plan:** `crates/hammer-service/src/tun/mod.rs` stores `Frame<Pending>` in `TunPendingTx` and adds `unsafe impl Send`. This violates the scheduler-owned pending-frame model and needs a separate approved TUN/frame handoff design or rollback.
- **Out of plan:** `Frame<Next>` / `Frame<Pending>` accessors in `crates/hammer-adapter/src/buffer.rs` still use `unreachable!()` after an owner is consumed. Data-plane owner access must be total over live owners or return a typed error; it must not rely on panic/unreachable/string sentinels.
- **Out of plan:** `crates/hammer-service/src/session/node.rs::SessionQueueOutput::enqueue_frame` keeps an obsolete `NodeId` argument as `_`. A `Frame<Next>` already owns its selected next node; compatibility parameters must be deleted from signatures and call sites, not swallowed with `_`.
- **Out of plan:** session queue dispatch failure accounting through `record_current_node_error` may be a reasonable node-runtime feature, but it is not part of this memory-management plan. Do not expand it into `NodeResult::error` here.
- **Already cleaned up:** `NodeResult::error`, `NodeNextFrames`, `NextFrame`, `append_to_frame`, `submit_frame`, `SegmentAllocOwner`, `RawAllocation`, and `PooledBufferMut` are not present in current production code. Do not reintroduce them.

### Correction Plan Before More Implementation

- [x] Delete pending-process dispatch and route scheduled frames through normal node processing. Do not export a pending-process callback alias through `hammer-adapter/src/lib.rs`, descriptors, or public registration APIs.
- [ ] Privatize or delete `MemoryConfig`, `MemoryMain`, and `StaticNumaTable`; `DataPlaneBufferConfig` must not expose a public fixed NUMA table field.
- [ ] Delete obsolete compatibility parameters such as `_: NodeId` / `_node: NodeId` / `_next: NodeId` from migrated APIs and update all callers in the same task.
- [ ] Keep `NodeResult` only as a dispatch outcome. Do not reintroduce `NodeResult::error`, `NodeResult::next_frame`, `NodeResult::next_current`, `NodeNextFrames`, `NextFrame`, `NodeNextFrame`, `Current(NodeId)`, `append_to_frame`, `enqueue_to_next_frames`, or any renamed staging/result-carrier equivalent.
- [ ] Rework or roll back TUN pending-frame storage so `Frame<Pending>` is not persisted outside node dispatch and is not made `Send`.
- [ ] Remove frame-owner accessor `unreachable!()` / `expect` / `unwrap` paths. Access over live owners must be structurally total, or the method must return a typed `DataPlaneError`.
- [ ] Keep `BufferIndexList` as a raw internal frame-index list for now. The current binding decision is "raw memory for allocator/buffer metadata, no Vec-backed helper"; do not replace it with `std::vec::Vec` or `hammer_infra::vec::Vec` for this memory slice.
- [ ] Treat node error propagation as a separate plan. This memory plan must not add `NodeResult::error(CoreError)` or change the graph result contract.
- [ ] After cleanup, run only focused checks: `cargo check -p hammer-service`, `cargo test -p hammer-adapter --test buffer_frame_guard -- --nocapture`, and `cargo test -p hammer-service --test frame_owner_cleanup_hot_path -- --nocapture`. Do not run `cargo test --workspace` during this cleanup pass.

---

### Task 1: Lock In Concrete Heap Foundation

**Files:**
- Modify: `Cargo.toml`
- Modify: `crates/hammer-infra/Cargo.toml`
- Modify: `crates/hammer-infra/src/heap.rs`
- Modify: `crates/hammer-infra/src/svm_region.rs`
- Modify: `crates/hammer-infra/src/segment.rs`
- Modify: `crates/hammer-infra/src/fifo.rs`
- Modify: `crates/hammer-infra/src/bihash/{alloc.rs,split.rs}`
- Modify: `crates/hammer-infra/src/lib.rs`
- Test: `crates/hammer-infra/tests/heap.rs`

**Interfaces:**
- Consumes: existing `SvmRegion` mmap region.
- Produces: concrete public `Heap`, private `HeapVTable`, `SvmRegion`, `mimalloc::MiMalloc` main-heap allocation, and no public heap trait.

- [ ] **Step 1: Write/adjust the failing heap test**

Ensure `crates/hammer-infra/tests/heap.rs` contains these API-shape checks in addition to allocation round trips:

```rust
use std::alloc::{GlobalAlloc, Layout};
use std::sync::Arc;

use hammer_infra::heap::Heap;
use hammer_infra::svm_region::SvmRegion;

#[test]
fn heap_api_is_concrete_handle_only() {
    let src = include_str!("../src/heap.rs");
    for forbidden in [
        "pub trait Heap",
        "HeapLocal",
        "HeapSvm",
        "H: Heap",
        "dyn GlobalAlloc",
        "#[global_allocator]",
        "std::alloc::Allocator",
        "std::alloc::System",
        "pub struct HeapRegistry",
        "pub unsafe fn dealloc",
        "pub fn numa_node",
        "BumpMainHeap",
        "MainHeapFreeList",
        "pub fn free",
        "pub fn freelist_len",
        "fn local_alloc",
        "fn local_dealloc",
        "fn local_numa",
        "fn svm_alloc",
        "fn svm_dealloc",
        "fn svm_numa",
        "fn alloc_region",
        "fn page_size",
        "unreachable!",
    ] {
        assert!(
            !src.contains(forbidden),
            "heap.rs must not expose {forbidden}"
        );
    }
    let trait_object_spell = format!("{} {}", "dyn", "Heap");
    assert!(
        !src.contains(&trait_object_spell),
        "Heap must stay a concrete handle, not a trait object"
    );
}

#[test]
fn svm_region_uses_talc_not_private_span_lists() {
    let src = include_str!("../src/svm_region.rs");
    assert!(src.contains("talc::TalcLock"));
    assert!(src.contains("source::Manual"));
    for forbidden in [
        format!("{}{}", "Returned", "Span"),
        format!("{}{}", "returned", "_spans"),
        format!("{}{}", "bump", "_depth"),
        format!("SVM_{}{}", "RETURNED_SPAN", "_NODES"),
        "std::vec::Vec".to_string(),
        "hammer_infra::vec::Vec".to_string(),
    ] {
        assert!(
            !src.contains(&forbidden),
            "svm_region.rs must not hand-roll allocator metadata: {forbidden}"
        );
    }
}

#[test]
fn heap_main_mimalloc_alloc_round_trip() {
    let src = include_str!("../src/heap.rs");
    assert!(src.contains("mimalloc::MiMalloc"));
    assert!(!src.contains("#[global_allocator]"));

    let heap = Heap::main();
    assert!(heap.is_main_heap());
    let layout = Layout::from_size_align(128, 64).unwrap();
    let ptr = heap.alloc(layout).expect("alloc");
    unsafe {
        std::ptr::write_bytes(ptr.as_ptr(), 0xAB, layout.size());
        GlobalAlloc::dealloc(&heap, ptr.as_ptr(), layout);
    }
}

#[test]
fn heap_local_is_main_heap_alias() {
    let heap = Heap::local();
    assert!(heap.is_main_heap());
}

#[test]
fn heap_svm_allocation_is_returned_by_drop() {
    let region = SvmRegion::try_with_size(48 * 1024).expect("create SVM region");
    let heap = Heap::svm_data(region.clone()).expect("create SVM data heap");
    let layout = Layout::from_size_align(32 * 1024, 64).unwrap();
    let start = region.base() as usize;
    let end = start + region.size();

    {
        let ptr = heap.alloc(layout).expect("alloc");
        let ptr_addr = ptr.as_ptr() as usize;
        assert!(ptr_addr >= start, "allocation must start inside SVM region");
        assert!(
            ptr_addr + layout.size() <= end,
            "allocation must end inside SVM region"
        );
        unsafe {
            GlobalAlloc::dealloc(&heap, ptr.as_ptr(), layout);
        }
    }

    {
        let ptr = heap.alloc(layout).expect("alloc");
        let ptr_addr = ptr.as_ptr() as usize;
        assert!(ptr_addr >= start, "allocation must start inside SVM region");
        assert!(
            ptr_addr + layout.size() <= end,
            "allocation must end inside SVM region"
        );
        unsafe {
            std::ptr::write_bytes(ptr.as_ptr(), 0xCD, layout.size());
            GlobalAlloc::dealloc(&heap, ptr.as_ptr(), layout);
        }
    }
}

#[test]
fn heap_clone_shares_svm_region() {
    let region = SvmRegion::try_with_size(1 << 16).expect("create SVM region");
    let heap = Arc::new(Heap::svm_data(region).expect("create SVM data heap"));
    let clone = heap.clone();
    drop(heap);
    assert!(clone.region().expect("clone keeps region alive").size() > 0);
}

#[test]
fn svm_region_maps_fd_backed_shared_memory() {
    let region = SvmRegion::try_with_size(4096).expect("create SVM region");
    assert!(region.size() >= 4096);
    assert!(region.fd() >= 0);

    let offset = region.alloc(16, 8);
    assert_ne!(offset, u64::MAX);
    unsafe {
        std::ptr::write_bytes(region.base().add(offset as usize), 0x5A, 16);
    }

    let attached = SvmRegion::from_fd(region.fd(), region.size()).expect("attach fd");
    unsafe {
        assert_eq!(*attached.base().add(offset as usize), 0x5A);
    }
}

#[test]
fn attached_svm_region_is_not_an_allocator_owner() {
    let region = SvmRegion::try_with_size(4096).expect("create SVM region");
    let attached = SvmRegion::from_fd(region.fd(), region.size()).expect("attach fd");

    assert_eq!(
        attached.alloc(16, 8),
        u64::MAX,
        "attached mappings can read/write existing offsets but cannot allocate"
    );
    assert!(
        Heap::svm_data(attached).is_err(),
        "attached mapping must not become an allocation-owner data heap"
    );
}

```

- [ ] **Step 2: Run the heap test red**

Run: `cargo test -p hammer-infra --test heap -- --nocapture`

Expected before the fix: failure if the old trait-based API, `HeapLocal`, `HeapSvm`, generic `H: Heap`, `dyn GlobalAlloc`, `#[global_allocator]`, `std::alloc::Allocator`, `std::alloc::System`, any hand-written main-heap allocator text remains in `crates/hammer-infra/src/heap.rs`, or any private span-list allocator remains in `crates/hammer-infra/src/svm_region.rs`.

- [ ] **Step 3: Add mature main-heap backend dependency wiring**

In workspace `Cargo.toml`, add the mature allocators as workspace dependencies:

```toml
mimalloc = { version = "0.1", default-features = false }
talc = { version = "5", default-features = false }
spinning_top = "0.3"
```

In `crates/hammer-infra/Cargo.toml`, add the direct dependency:

```toml
[dependencies]
crossbeam-utils = { workspace = true }
libc = { workspace = true }
mimalloc = { workspace = true }
talc = { workspace = true }
spinning_top = { workspace = true }
```

Implementation requirements:

- `mimalloc::MiMalloc` is the default and only planned main-heap backend in this task.
- `talc` is the only planned SVM span allocator in this task. It manages caller-provided mapped memory through `source::Manual`; Hammer owns mmap/fd lifetime and Talc owns allocation bookkeeping for the owner mapping.
- `spinning_top::RawSpinlock` is used only as Talc's allocator lock primitive. Do not use `std::sync::Mutex` for SVM allocator internals.
- Do not install mimalloc as Rust's process-wide global allocator. Hammer selects it through the explicit `Heap` handle and private vtable only.
- Do not add `jemalloc`, `snmalloc`, `rpmalloc`, or another allocator in this task. The point is to make the backend explicit and swappable without expanding the dependency surface.

- [ ] **Step 4: Implement the concrete heap handle**

In `crates/hammer-infra/src/heap.rs`, keep this public shape, then implement it with the concrete allocator body immediately below. `HeapVTable` is shown only to define the private implementation shape; it must not be public.

```rust
#[repr(C)]
struct HeapVTable {
    alloc: unsafe fn(*const (), Layout) -> *mut u8,
    dealloc: unsafe fn(*const (), *mut u8, Layout),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeapError {
    AttachedSvmRegion,
}

pub struct Heap {
    vt: &'static HeapVTable,
    data: Arc<HeapData>,
}

impl Heap {
    pub fn main() -> Heap;
    pub fn local() -> Heap;
    pub fn svm_data(region: SvmRegion) -> Result<Heap, HeapError>;
    pub fn alloc(&self, layout: Layout) -> Option<NonNull<u8>>;
    pub fn is_main_heap(&self) -> bool;
    pub fn region(&self) -> Option<&SvmRegion>;
}

```

Implementation requirements:

- `#[derive(Clone)]` or an explicit `Clone` implementation clones the retained `Arc<HeapData>`. Do not use `Arc::into_raw` / `Arc::from_raw` unless the implementation genuinely needs erased ownership; the public handle is already concrete.
- Do not add a public `Heap::dealloc` method. Deallocation is reachable only through `GlobalAlloc for Heap`, and Hammer code uses that from owning container/arena `Drop` implementations.
- `Heap::main()` calls `mimalloc::MiMalloc` directly through the private vtable. It must not call `std::alloc::alloc` or `std::alloc::System`, because either would hide the chosen main-heap backend.
- `Heap::local()` remains as a thin alias for `Heap::main()` so existing local-heap call sites keep compiling while the actual backend becomes mature allocator-backed.
- `Heap::svm_data(region)` returns `Err(HeapError::AttachedSvmRegion)` when passed an attached mapping. It must not assert, panic, or embed a string error.
- The SVM vtable dealloc passes the allocation pointer and `Layout` to the crate-private Talc-backed SVM allocator path. It must not compute offsets into a hand-written returned-span list.

Use this allocator body. The important part is that `HeapData`, `MAIN_VT`, and `SVM_VT` are private; backend logic lives inside the vtable definitions, not in a pile of top-level `svm_*` / `local_*` functions.

```rust
use std::alloc::{GlobalAlloc, Layout};
use std::ptr::NonNull;
use std::sync::Arc;

use crate::svm_region::SvmRegion;

enum HeapData {
    Main,
    SvmData { region: SvmRegion },
}

#[derive(Clone)]
pub struct Heap {
    vt: &'static HeapVTable,
    data: Arc<HeapData>,
}

static MIMALLOC_ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

impl Heap {
    pub fn main() -> Heap {
        Heap {
            vt: &MAIN_VT,
            data: Arc::new(HeapData::Main),
        }
    }

    pub fn local() -> Heap {
        Self::main()
    }

    pub fn svm_data(region: SvmRegion) -> Result<Heap, HeapError> {
        if !region.is_allocation_owner() {
            return Err(HeapError::AttachedSvmRegion);
        }
        Ok(Heap {
            vt: &SVM_VT,
            data: Arc::new(HeapData::SvmData { region }),
        })
    }

    #[inline]
    pub fn alloc(&self, layout: Layout) -> Option<NonNull<u8>> {
        unsafe { NonNull::new((self.vt.alloc)(self.data_ptr(), layout)) }
    }

    pub fn is_main_heap(&self) -> bool {
        matches!(self.data.as_ref(), HeapData::Main)
    }

    pub fn region(&self) -> Option<&SvmRegion> {
        match self.data.as_ref() {
            HeapData::SvmData { region } => Some(region),
            HeapData::Main => None,
        }
    }

    #[inline]
    fn data_ptr(&self) -> *const () {
        Arc::as_ptr(&self.data).cast::<()>()
    }
}

unsafe impl GlobalAlloc for Heap {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        unsafe { (self.vt.alloc)(self.data_ptr(), layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { (self.vt.dealloc)(self.data_ptr(), ptr, layout) }
    }
}

static MAIN_VT: HeapVTable = HeapVTable {
    alloc: |_, layout| {
        if layout.size() == 0 {
            return layout.align() as *mut u8;
        }
        unsafe { GlobalAlloc::alloc(&MIMALLOC_ALLOC, layout) }
    },
    dealloc: |_, ptr, layout| {
        if layout.size() == 0 {
            return;
        }
        unsafe { GlobalAlloc::dealloc(&MIMALLOC_ALLOC, ptr, layout) }
    },
};

static SVM_VT: HeapVTable = HeapVTable {
    alloc: |data, layout| {
        if layout.size() == 0 {
            return layout.align() as *mut u8;
        }
        let HeapData::SvmData { region } = (unsafe { &*(data as *const HeapData) }) else {
            return std::ptr::null_mut();
        };
        region
            .alloc_layout(layout)
            .map_or(std::ptr::null_mut(), NonNull::as_ptr)
    },
    dealloc: |data, ptr, layout| {
        if layout.size() == 0 {
            return;
        }
        let HeapData::SvmData { region } = (unsafe { &*(data as *const HeapData) }) else {
            return;
        };
        if let Some(ptr) = NonNull::new(ptr) {
            unsafe {
                region.dealloc_layout(ptr, layout);
            }
        }
    },
};

```

Additional allocator requirements:

- `Heap::main` and `Heap::local` allocate through the single mature `mimalloc::MiMalloc` main-heap backend. `Heap::svm_data` owns a cloned `SvmRegion` handle in `HeapData` so the mmap/fd lifetime follows the heap handle.
- Zero-sized layouts return an aligned dangling pointer and deallocate as a no-op.
- The SVM vtable's `dealloc` branch returns the pointer and `Layout` to `SvmRegion`'s Talc allocator. No caller outside allocator/container `Drop` code can trigger this directly.
- Do not add top-level backend functions named `local_alloc`, `local_dealloc`, `local_numa`, `svm_alloc`, `svm_dealloc`, or `svm_numa`; keep backend behavior colocated with `MAIN_VT` and `SVM_VT`.

- [ ] **Step 5: Implement the SVM region allocator**

In `crates/hammer-infra/src/svm_region.rs`, implement the reusable SVM storage as fd-backed mmap memory plus a mature allocator over that mapped span. This is deliberately not a public heap type; it is the backing region used by `Heap::svm_data`.

```rust
use std::alloc::{GlobalAlloc, Layout};
use std::ffi::CString;
use std::os::fd::RawFd;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use spinning_top::RawSpinlock;
use talc::{source::Manual, TalcLock};

use crate::align::align_up;

type SvmAllocator = TalcLock<RawSpinlock, Manual>;

#[derive(Debug)]
pub enum SvmRegionError {
    PageSizeUnavailable,
    NameContainsNul,
    MemfdCreate(std::io::Error),
    ShmOpen(std::io::Error),
    Ftruncate(std::io::Error),
    Mmap(std::io::Error),
    ClaimAllocator,
}

pub struct SvmRegion {
    inner: Arc<SvmRegionInner>,
}

struct SvmRegionInner {
    base: *mut u8,
    size: usize,
    fd: RawFd,
    owned: bool,
    allocator: Option<SvmAllocator>,
}

unsafe impl Send for SvmRegionInner {}
unsafe impl Sync for SvmRegionInner {}

static SVM_REGION_COUNTER: AtomicU64 = AtomicU64::new(0);

impl Clone for SvmRegion {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl SvmRegion {
    pub fn try_with_size(size: usize) -> Result<SvmRegion, SvmRegionError> {
        let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        if page <= 0 {
            return Err(SvmRegionError::PageSizeUnavailable);
        }
        let total = align_up(size, page as usize);
        let counter = SVM_REGION_COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();

        #[cfg(target_os = "linux")]
        let (base, fd, owned) = {
            let name = CString::new(format!("hammer-region-{pid}-{counter}"))
                .map_err(|_| SvmRegionError::NameContainsNul)?;
            let fd = unsafe { libc::memfd_create(name.as_ptr(), libc::MFD_CLOEXEC) };
            if fd < 0 {
                return Err(SvmRegionError::MemfdCreate(std::io::Error::last_os_error()));
            }

            let ret = unsafe { libc::ftruncate(fd, total as libc::off_t) };
            if ret != 0 {
                let error = std::io::Error::last_os_error();
                unsafe { libc::close(fd) };
                return Err(SvmRegionError::Ftruncate(error));
            }

            let base = unsafe {
                libc::mmap(
                    std::ptr::null_mut(),
                    total,
                    libc::PROT_READ | libc::PROT_WRITE,
                    libc::MAP_SHARED,
                    fd,
                    0,
                )
            };
            if base == libc::MAP_FAILED {
                let error = std::io::Error::last_os_error();
                unsafe { libc::close(fd) };
                return Err(SvmRegionError::Mmap(error));
            }

            (base.cast(), fd, true)
        };

        #[cfg(not(target_os = "linux"))]
        let (base, fd, owned) = {
            let name = CString::new(format!("/hammer-region-{pid}-{counter}"))
                .map_err(|_| SvmRegionError::NameContainsNul)?;
            let fd = unsafe { libc::shm_open(name.as_ptr(), libc::O_CREAT | libc::O_RDWR, 0o600) };
            if fd < 0 {
                return Err(SvmRegionError::ShmOpen(std::io::Error::last_os_error()));
            }
            unsafe { libc::shm_unlink(name.as_ptr()) };

            let ret = unsafe { libc::ftruncate(fd, total as libc::off_t) };
            if ret != 0 {
                let error = std::io::Error::last_os_error();
                unsafe { libc::close(fd) };
                return Err(SvmRegionError::Ftruncate(error));
            }

            let base = unsafe {
                libc::mmap(
                    std::ptr::null_mut(),
                    total,
                    libc::PROT_READ | libc::PROT_WRITE,
                    libc::MAP_SHARED,
                    fd,
                    0,
                )
            };
            if base == libc::MAP_FAILED {
                let error = std::io::Error::last_os_error();
                unsafe { libc::close(fd) };
                return Err(SvmRegionError::Mmap(error));
            }

            (base.cast(), fd, true)
        };

        let allocator = match Self::claim_allocator(base, total) {
            Some(allocator) => Some(allocator),
            None => {
                unsafe {
                    libc::munmap(base.cast(), total);
                    libc::close(fd);
                }
                return Err(SvmRegionError::ClaimAllocator);
            }
        };

        Ok(SvmRegion {
            inner: Arc::new(SvmRegionInner {
                base,
                size: total,
                fd,
                owned,
                allocator,
            }),
        })
    }

    pub fn from_fd(fd: RawFd, size: usize) -> Option<SvmRegion> {
        Self::from_fd_owned(fd, size, false)
    }

    pub fn from_fd_owned(fd: RawFd, size: usize, owned: bool) -> Option<SvmRegion> {
        let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        assert!(page > 0, "sysconf(_SC_PAGESIZE) must return a positive page size");
        let total = align_up(size, page as usize);
        let base = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                total,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd,
                0,
            )
        };
        if base == libc::MAP_FAILED {
            if owned {
                unsafe { libc::close(fd) };
            }
            return None;
        }

        Some(SvmRegion {
            inner: Arc::new(SvmRegionInner {
                base: base.cast(),
                size: total,
                fd,
                owned,
                allocator: None,
            }),
        })
    }

    pub(crate) fn from_created_fd_owned(fd: RawFd, size: usize) -> Option<SvmRegion> {
        let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        assert!(page > 0, "sysconf(_SC_PAGESIZE) must return a positive page size");
        let total = align_up(size, page as usize);
        let base = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                total,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd,
                0,
            )
        };
        if base == libc::MAP_FAILED {
            unsafe { libc::close(fd) };
            return None;
        }
        let base = base.cast();
        let allocator = match Self::claim_allocator(base, total) {
            Some(allocator) => Some(allocator),
            None => {
                unsafe {
                    libc::munmap(base.cast(), total);
                    libc::close(fd);
                }
                return None;
            }
        };
        Some(SvmRegion {
            inner: Arc::new(SvmRegionInner {
                base,
                size: total,
                fd,
                owned: true,
                allocator,
            }),
        })
    }

    pub fn base(&self) -> *mut u8 {
        self.inner.base
    }

    pub fn size(&self) -> usize {
        self.inner.size
    }

    pub fn fd(&self) -> RawFd {
        self.inner.fd
    }

    pub(crate) fn is_allocation_owner(&self) -> bool {
        self.inner.allocator.is_some()
    }

    pub fn alloc(&self, bytes: usize, align: usize) -> u64 {
        if bytes == 0 {
            return 0;
        }
        let Some(allocator) = self.inner.allocator.as_ref() else {
            return u64::MAX;
        };
        let Ok(layout) = Layout::from_size_align(bytes, align) else {
            return u64::MAX;
        };
        let ptr = unsafe { GlobalAlloc::alloc(allocator, layout) };
        if ptr.is_null() {
            return u64::MAX;
        }
        debug_assert!(self.contains(ptr, bytes));
        (ptr as usize - self.inner.base as usize) as u64
    }

    pub(crate) fn alloc_layout(&self, layout: Layout) -> Option<NonNull<u8>> {
        if layout.size() == 0 {
            return NonNull::new(layout.align() as *mut u8);
        }
        let allocator = self.inner.allocator.as_ref()?;
        let ptr = unsafe { GlobalAlloc::alloc(allocator, layout) };
        NonNull::new(ptr)
    }

    pub(crate) unsafe fn dealloc_layout(&self, ptr: NonNull<u8>, layout: Layout) {
        if layout.size() == 0 {
            return;
        }
        debug_assert!(self.contains(ptr.as_ptr(), layout.size()));
        let Some(allocator) = self.inner.allocator.as_ref() else {
            return;
        };
        unsafe {
            GlobalAlloc::dealloc(allocator, ptr.as_ptr(), layout);
        }
    }

    fn contains(&self, ptr: *mut u8, bytes: usize) -> bool {
        let start = self.inner.base as usize;
        let end = start + self.inner.size;
        let ptr = ptr as usize;
        ptr >= start && ptr.checked_add(bytes).is_some_and(|end_ptr| end_ptr <= end)
    }

    fn claim_allocator(base: *mut u8, size: usize) -> Option<SvmAllocator> {
        let mut allocator = TalcLock::<RawSpinlock, Manual>::new(Manual);
        unsafe {
            allocator
                .get_mut()
                .claim(base, size)
                .ok()?;
        }
        Some(allocator)
    }
}

impl Drop for SvmRegionInner {
    fn drop(&mut self) {
        unsafe {
            if !self.base.is_null() {
                libc::munmap(self.base.cast(), self.size);
            }
            if self.owned {
                libc::close(self.fd);
            }
        }
    }
}

```

Implementation requirements:

- `SvmRegion::try_with_size` owns the platform split shown above directly: Linux uses `memfd_create + ftruncate + mmap`; other Unix targets use `shm_open + shm_unlink + ftruncate + mmap`. Do not add a top-level `alloc_region` helper.
- Every fd-creation failure returns `SvmRegionError::{MemfdCreate|ShmOpen}(std::io::Error::last_os_error())`. Every failure after fd creation closes the fd before returning the typed error.
- `try_with_size` creates the allocation-owner region and immediately calls `TalcLock::<RawSpinlock, Manual>::new(Manual)` plus `claim(base, total)` on the mapped span.
- If Talc cannot claim the mapped span, `try_with_size` must `munmap`, `close`, and return `SvmRegionError::ClaimAllocator`; `from_created_fd_owned` must `munmap`, `close`, and return `None`.
- `from_fd` / `from_fd_owned` create attach mappings only. They must not create a second allocator over the same shared memory. `alloc` on an attached mapping returns `u64::MAX`, and `Heap::svm_data` returns `Err(HeapError::AttachedSvmRegion)`.
- This attach-only rule is a scoped non-VPP behavior. Local VPP attachers can allocate through `ssvm_mem_alloc`; Hammer attachers cannot in this plan.
- `from_fd_owned(fd, size, owned=false)` must not close the caller-owned fd on `mmap` failure or `SvmRegion` drop. `try_with_size` and `from_created_fd_owned(fd, size)` must close the fd on `mmap` failure and after `munmap`.
- `from_created_fd_owned` is crate-private and exists only for code that has just created/truncated a fresh fd-backed region and must install the owner allocator. It is not a caller-facing allocation API.
- `alloc` constructs a `Layout` from `(bytes, align)`, delegates to Talc through `GlobalAlloc::alloc`, returns an offset from `base`, and returns `u64::MAX` on invalid layout, OOM, or attached mapping.
- `dealloc_layout` is crate-private and is called only by the `Heap` vtable's `dealloc` callback. It delegates to Talc through `GlobalAlloc::dealloc` and returns immediately if called on an attached mapping.
- Do not add SVM allocator state named `bump`, hand-written span node lists, side `mmap` metadata arrays, or `std::sync::Mutex` in `svm_region.rs`.
- Do not add top-level helper functions named `alloc_region` or `page_size`; keep this allocation path visible inside `SvmRegion::try_with_size` / `from_fd_owned`.

- [ ] **Step 6: Keep `Svm` as a thin `SvmRegion` wrapper without a public return path**

`crates/hammer-infra/src/segment.rs` must remove the old direct return method from `Segment`; `Svm` delegates allocation and fd access to `SvmRegion` only:

```rust
pub trait Segment: Send + Sync + Clone + 'static {
    fn base(&self) -> *mut u8;
    fn alloc(&self, bytes: usize, align: usize) -> u64;
    fn fd(&self) -> Option<RawFd>;
}

pub struct Svm {
    region: SvmRegion,
}

impl Segment for Svm {
    fn base(&self) -> *mut u8 { self.region.base() }
    fn alloc(&self, bytes: usize, align: usize) -> u64 { self.region.alloc(bytes, align) }
    fn fd(&self) -> Option<RawFd> { Some(self.region.fd()) }
}
```

Any infra structure that previously called `Segment::free` must move ownership into its own `Drop` path and deallocate through the retained `Heap` handle. Do not add a replacement direct lifetime method to `Segment`, and do not add per-structure span caches.

Implementation requirements:

- `Svm::create` uses the crate-private `SvmRegion::from_created_fd_owned` after it creates/truncates a fresh named shared-memory fd, so the creator gets the Talc owner allocator.
- `Svm::from_fd` remains attach-only and must not allocate from the mapped segment. It exists for consumers that read/write objects already laid out by the owner using offsets.
- Tests that fork or attach through `Svm::from_fd` should verify shared bytes are visible, not that the attached side can allocate.

- [ ] **Step 7: Run green and commit**

Run:

```bash
cargo test -p hammer-infra --test heap -- --nocapture
cargo test -p hammer-infra segment -- --nocapture
cargo fmt --all -- --check
```

Expected: all commands pass.

Commit:

```bash
git add Cargo.toml crates/hammer-infra/Cargo.toml \
        crates/hammer-infra/src/heap.rs crates/hammer-infra/src/svm_region.rs \
        crates/hammer-infra/src/segment.rs crates/hammer-infra/src/fifo.rs \
        crates/hammer-infra/src/bihash/alloc.rs crates/hammer-infra/src/bihash/split.rs \
        crates/hammer-infra/src/lib.rs \
        crates/hammer-infra/tests/heap.rs
git commit -m "hammer-infra(Feat): concrete per-numa Heap handle"
```

---

### Task 2: Route Raw Infra Containers Through `Arc<Heap>`

**Files:**
- Modify: `crates/hammer-infra/src/pool.rs`
- Modify: `crates/hammer-infra/src/boxed.rs`
- Modify: `crates/hammer-infra/src/map.rs`
- Test: `crates/hammer-infra/tests/pool_heap.rs`
- Test: `crates/hammer-infra/tests/slice_map_heap.rs`

**Interfaces:**
- Consumes: concrete `Arc<Heap>` from Task 1.
- Produces: heap-aware raw allocation constructors that retain the same `Arc<Heap>` for deallocation without using Vec-backed bookkeeping.

- [ ] **Step 1: Write/adjust the failing container tests**

`crates/hammer-infra/tests/pool_heap.rs` must prove `Pool<T>` allocates and deallocates through `Heap::svm_data`:

```rust
use std::sync::Arc;

use hammer_infra::heap::Heap;
use hammer_infra::pool::Pool;
use hammer_infra::svm_region::SvmRegion;

#[test]
fn pool_with_capacity_in_routes_through_heap_svm() {
    let region = SvmRegion::try_with_size(64 * 1024).expect("create SVM region");
    let heap = Arc::new(Heap::svm_data(region).expect("create SVM data heap"));

    {
        let mut pool: Pool<u64> = Pool::with_capacity_in(2048, heap.clone());
        let index = pool.insert(42).expect("insert");
        assert_eq!(pool.get(index), Some(&42));
    }

    {
        let mut pool: Pool<u64> = Pool::with_capacity_in(2048, heap);
        let index = pool.insert(7).expect("insert again");
        assert_eq!(pool.get(index), Some(&7));
    }
}
```

`crates/hammer-infra/tests/slice_map_heap.rs` must cover `Slice` and `FlatHashTable`:

```rust
#![allow(deprecated)]

use std::sync::Arc;

use hammer_infra::boxed::Slice;
use hammer_infra::heap::Heap;
use hammer_infra::map::FlatHashTable;
use hammer_infra::svm_region::SvmRegion;

#[test]
fn slice_and_flat_hash_table_use_the_passed_heap() {
    let region = SvmRegion::try_with_size(128 * 1024).expect("create SVM region");
    let heap = Arc::new(Heap::svm_data(region).expect("create SVM data heap"));

    let slice = Slice::from_elem_in(2048, 0u8, heap.clone());
    let mut table: FlatHashTable<u64, u64> = FlatHashTable::with_capacity_in(64, heap);
    table.insert(1, 100);

    assert_eq!(slice.len(), 2048);
    assert_eq!(table.get(&1), Some(&100));
}

#[test]
fn infra_container_sources_do_not_reintroduce_heap_traits() {
    for src in [
        include_str!("../src/pool.rs"),
        include_str!("../src/boxed.rs"),
        include_str!("../src/map.rs"),
    ] {
        assert!(!src.contains("H: Heap"));
        assert!(!src.contains("pub trait Heap"));
        assert!(src.contains("Arc<Heap>") || src.contains("&Heap"));
    }
}
```

- [ ] **Step 2: Run tests red**

Run:

```bash
cargo test -p hammer-infra --test pool_heap -- --nocapture
cargo test -p hammer-infra --test slice_map_heap -- --nocapture
```

Expected before the fix: missing `*_in(..., Arc<Heap>)` constructors or source-shape failures.

- [ ] **Step 3: Implement heap retention**

Use these exact constructor signatures:

```rust
impl<T, const ALIGN: usize> Pool<T, ALIGN> {
    pub fn with_capacity(capacity: usize) -> Self;
    pub fn with_capacity_in(capacity: usize, heap: Arc<Heap>) -> Self;
}

impl<T, const ALIGN: usize> Slice<T, ALIGN> {
    pub fn from_elem(len: usize, value: T) -> Self
    where
        T: Clone;

    pub fn from_elem_in(len: usize, value: T, heap: Arc<Heap>) -> Self
    where
        T: Clone;
}

impl<K: FlatHashKey, V: Clone> FlatHashTable<K, V> {
    pub fn with_capacity(capacity: usize) -> Self;
    pub fn with_capacity_in(capacity: usize, heap: Arc<Heap>) -> Self;
}
```

Implementation requirements:

- Default constructors use `Arc::new(Heap::local())`.
- Each container stores the allocation heap and deallocates through the same handle in `Drop`.
- `Pool` free slots, `Slice` storage, and `FlatHashTable` buckets use raw pointer allocations from the retained heap. They must not use `std::vec::Vec` or `hammer_infra::vec::Vec` for bookkeeping.
- `FlatHashTable::grow` allocates replacement bucket storage from the retained heap.

- [ ] **Step 4: Run green and commit**

Run:

```bash
cargo test -p hammer-infra --test pool_heap -- --nocapture
cargo test -p hammer-infra --test slice_map_heap -- --nocapture
cargo test -p hammer-infra
cargo fmt --all -- --check
```

Expected: all commands pass.

Commit:

```bash
git add crates/hammer-infra/src/pool.rs \
        crates/hammer-infra/src/boxed.rs crates/hammer-infra/src/map.rs \
        crates/hammer-infra/tests/pool_heap.rs crates/hammer-infra/tests/slice_map_heap.rs
git commit -m "hammer-infra(Feat): containers allocate through concrete Heap"
```

---

### Task 3: Inline Buffer Slot Layout

**Files:**
- Modify: `crates/hammer-adapter/src/buffer.rs`
- Test: `crates/hammer-adapter/tests/buffer_inline_layout.rs`

**Interfaces:**
- Consumes: `Arc<Heap>` and `buffer_data_offset()`.
- Produces: contiguous slot storage with `Buffer` headers and data in one heap allocation.

- [ ] **Step 1: Write/adjust the failing layout test**

Use this exact test file:

```rust
use std::sync::Arc;

use hammer_adapter::buffer::{
    BufferPool, BufferPoolArena, BUFFER_THREAD_CACHE_BATCH, BUFFER_THREAD_CACHE_HIGH_WATER,
    DEFAULT_PRE_DATA_SIZE, buffer_data_offset,
};
use hammer_infra::heap::Heap;
use hammer_infra::svm_region::SvmRegion;

#[test]
fn one_contiguous_region_header_and_data_inline() {
    let pool = BufferPool::with_capacity(2048, 64);
    let index = pool.alloc_index().expect("alloc");

    pool.append(index, &[0xAB; 100]).expect("append");

    let buffer = pool.get(index).expect("buffer");
    let header_ptr = std::ptr::from_ref(&*buffer) as usize;
    let current_ptr = buffer.current_ptr() as usize;

    assert_eq!(current_ptr - header_ptr, buffer_data_offset());
    assert_eq!(buffer.current(), &[0xAB; 100]);
}

#[test]
fn index_to_pointer_is_slot_times_stride() {
    let pool = BufferPool::with_capacity(2048, 8);
    let stride = pool.slot_stride();
    let base = pool.base_ptr() as usize;
    let slot = 5u32;
    let got = pool.buffer_raw_ptr(slot) as usize;

    assert_eq!(got, base + slot as usize * stride);
}

#[test]
fn global_buffer_index_zero_is_never_a_valid_allocation() {
    let pool = BufferPool::with_capacity(2048, 8);
    let first = pool.alloc_index().expect("first alloc");
    assert_ne!(first.as_u64(), 0, "encoded BufferIndex zero must stay invalid");
    assert_eq!(pool.buffer_raw_ptr(first.slot()) as usize, pool.base_ptr() as usize + first.slot() as usize * pool.slot_stride());
}

#[test]
fn negative_current_data_points_into_pre_data_headroom() {
    let pool = BufferPool::with_capacity(2048, 32);
    let index = pool.alloc_index().expect("alloc");

    pool.append(index, &[0u8; 32]).expect("append payload");
    pool.prepend(index, &[0x42; 32]).expect("prepend header");

    let buffer = pool.get(index).expect("buffer");
    assert_eq!(buffer.current_data_offset(), -32);
    assert_eq!(&buffer.current()[..32], &[0x42; 32]);
    assert_eq!(
        pool.data_raw_ptr(index.slot()) as usize - buffer.current_ptr() as usize,
        32
    );
    assert_eq!(DEFAULT_PRE_DATA_SIZE, 128);
}

#[test]
fn per_thread_cache_constants_match_local_vpp() {
    assert_eq!(BUFFER_THREAD_CACHE_BATCH, 32);
    assert_eq!(BUFFER_THREAD_CACHE_HIGH_WATER, 512);
}

#[test]
fn arena_allocates_one_heap_region_and_returns_it_on_drop() {
    let region = SvmRegion::try_with_size(1 << 20).expect("create SVM region");
    let heap = Arc::new(Heap::svm_data(region.clone()).expect("create SVM data heap"));
    let region_start = region.base() as usize;
    let region_end = region_start + region.size();

    {
        let arena = BufferPoolArena::with_capacity_in(2048, 64, heap.clone(), 0);
        let pool = BufferPool::with_arena(arena);
        let base = pool.base_ptr() as usize;
        assert_eq!(base % 64, 0);
        assert!(base >= region_start);
        assert!(base < region_end);

        let first = pool.alloc_index().expect("alloc");
        assert_eq!(
            pool.buffer_raw_ptr(first.slot()) as usize,
            base + first.slot() as usize * pool.slot_stride()
        );
    }

    {
        let arena = BufferPoolArena::with_capacity_in(2048, 64, heap, 0);
        let pool = BufferPool::with_arena(arena);
        let base = pool.base_ptr() as usize;
        assert_eq!(base % 64, 0);
        assert!(base >= region_start);
        assert!(base < region_end);
    }
}
```

- [ ] **Step 2: Run the layout test red**

Run: `cargo test -p hammer-adapter --test buffer_inline_layout -- --nocapture`

Expected before the fix: missing raw pointer/stride accessors or old per-buffer `Slice<u8>` storage.

- [ ] **Step 3: Implement the slot layout**

In `crates/hammer-adapter/src/buffer.rs`, use this storage model:

```rust
pub const DEFAULT_PRE_DATA_SIZE: usize = 128;

#[repr(C, align(64))]
pub struct Buffer {
    cacheline0: BufferHeaderCacheline0,
    cacheline1: BufferHeaderCacheline1,
}

pub const fn buffer_data_offset() -> usize {
    core::mem::size_of::<Buffer>() + DEFAULT_PRE_DATA_SIZE
}

#[derive(Debug, Default, Clone, Copy)]
struct BufferSlot {
    generation: u32,
    allocated: bool,
}

struct BufferPoolInner {
    pool_id: u64,
    pool_registry_slot: u8,
    slot_capacity: usize,
    slot_stride: usize,
    numa_node: u32,
    region: Arc<Heap>,
    region_base: NonNull<u8>,
    region_layout: Layout,
    region_size: usize,
    metadata_heap: Arc<Heap>,
    metadata_base: NonNull<u8>,
    metadata_layout: Layout,
    slot_states: NonNull<BufferSlot>,
    available_stack: NonNull<u32>,
    available_len: usize,
    total_slots: usize,
    in_use: usize,
    in_use_delta: i32,
}
```

The arena allocation itself must be one heap allocation, not one allocation per buffer:

```rust
pub struct BufferPoolArena {
    inner: Rc<RefCell<BufferPoolInner>>,
}

impl BufferPoolArena {
    pub fn with_capacity(slot_capacity: usize, slots: usize) -> Self {
        Self::with_capacity_in(slot_capacity, slots, Arc::new(Heap::local()), 0)
    }

    pub fn with_capacity_in(slot_capacity: usize, slots: usize, heap: Arc<Heap>, numa_node: u32) -> Self {
        assert!(slot_capacity > 0, "buffer slot capacity must be non-zero");
        assert!(slots > 0, "buffer pool must contain at least one usable slot");

        let total_slots = slots;
        let slot_stride = align_up(
            buffer_data_offset()
                .checked_add(slot_capacity)
                .expect("buffer slot size overflow"),
            BUFFER_CACHE_LINE_SIZE,
        );
        let region_size = slot_stride
            .checked_mul(total_slots)
            .expect("buffer arena size overflow");
        let region_layout = Layout::from_size_align(region_size, BUFFER_CACHE_LINE_SIZE)
            .expect("buffer arena layout");
        let region_base = heap
            .alloc(region_layout)
            .expect("buffer arena heap allocation");

        unsafe {
            // The full arena is packet memory. Zero it once on creation so every
            // slot starts with deterministic header/pre-data/data bytes.
            ptr::write_bytes(region_base.as_ptr(), 0, region_layout.size());
        }

        let slot_state_bytes = core::mem::size_of::<BufferSlot>()
            .checked_mul(total_slots)
            .expect("buffer metadata slot state overflow");
        let available_bytes = core::mem::size_of::<u32>()
            .checked_mul(slots)
            .expect("buffer metadata availability overflow");
        let available_offset = align_up(slot_state_bytes, core::mem::align_of::<u32>());
        let metadata_size = available_offset
            .checked_add(available_bytes)
            .expect("buffer metadata size overflow");
        let metadata_layout = Layout::from_size_align(metadata_size, BUFFER_CACHE_LINE_SIZE)
            .expect("buffer metadata layout");
        let metadata_base = heap
            .alloc(metadata_layout)
            .expect("buffer metadata heap allocation");
        unsafe {
            ptr::write_bytes(metadata_base.as_ptr(), 0, metadata_layout.size());
        }
        let slot_states = metadata_base.cast::<BufferSlot>();
        let available_stack = unsafe {
            NonNull::new_unchecked(metadata_base.as_ptr().add(available_offset).cast::<u32>())
        };
        for i in 0..slots {
            let slot = u32::try_from(total_slots - i - 1).expect("buffer slot fits u32");
            unsafe { available_stack.as_ptr().add(i).write(slot) };
        }

        let pool_registry_slot = register_buffer_pool(slot_capacity);
        Self {
            inner: Rc::new(RefCell::new(BufferPoolInner {
                pool_id: next_buffer_pool_id(),
                pool_registry_slot,
                slot_capacity,
                slot_stride,
                numa_node,
                region: Arc::clone(&heap),
                region_base,
                region_layout,
                region_size,
                metadata_heap: heap,
                metadata_base,
                metadata_layout,
                slot_states,
                available_stack,
                available_len: slots,
                total_slots,
                in_use: 0,
                in_use_delta: 0,
            })),
        }
    }
}

impl Drop for BufferPoolInner {
    fn drop(&mut self) {
        unregister_buffer_pool(self.pool_registry_slot);
        unsafe {
            std::alloc::GlobalAlloc::dealloc(
                &*self.region,
                self.region_base.as_ptr(),
                self.region_layout,
            );
            std::alloc::GlobalAlloc::dealloc(
                &*self.metadata_heap,
                self.metadata_base.as_ptr(),
                self.metadata_layout,
            );
        }
    }
}
```

Slot pointer math must be O(1) and must never allocate:

```rust
impl BufferPoolInner {
    #[inline]
    fn slot_offset(&self, slot: u32) -> CoreResult<usize> {
        let slot = usize::try_from(slot).expect("buffer slot index fits usize");
        if slot >= self.total_slots {
            return Err(DataPlaneError::BufferSlotOutOfBounds.into());
        }
        slot.checked_mul(self.slot_stride)
            .ok_or(DataPlaneError::BufferSlotOffsetOverflow.into())
    }

    #[inline]
    fn buffer_raw_ptr(&self, slot: u32) -> CoreResult<*mut Buffer> {
        let offset = self.slot_offset(slot)?;
        Ok(unsafe { self.region_base.as_ptr().add(offset).cast::<Buffer>() })
    }

    #[inline]
    fn data_raw_ptr(&self, slot: u32) -> CoreResult<*mut u8> {
        let offset = self
            .slot_offset(slot)?
            .checked_add(buffer_data_offset())
            .ok_or(DataPlaneError::BufferDataPointerOverflow.into())?;
        Ok(unsafe { self.region_base.as_ptr().add(offset) })
    }

    #[inline]
    fn buffer_at_slot(&self, slot: u32) -> CoreResult<&Buffer> {
        Ok(unsafe { &*self.buffer_raw_ptr(slot)? })
    }

    #[inline]
    fn buffer_at_slot_mut(&mut self, slot: u32) -> CoreResult<&mut Buffer> {
        Ok(unsafe { &mut *self.buffer_raw_ptr(slot)? })
    }
}
```

Implementation requirements:

- `BufferPoolArena::with_capacity(slot_capacity, slots)` calls `BufferPoolArena::with_capacity_in(slot_capacity, slots, Arc::new(Heap::local()), 0)`.
- The `slots` parameter is the number of usable buffers. Arena storage and metadata allocate exactly `slots` entries. Do not reserve every pool's slot 0; VPP wastes at most one buffer globally only to keep encoded buffer index zero invalid.
- `BufferPoolArena::with_capacity_in` computes `slot_stride = align_up(buffer_data_offset() + slot_capacity, BUFFER_CACHE_LINE_SIZE)`.
- `BufferPoolArena::with_capacity_in` computes `region_layout = Layout::from_size_align(slot_stride * slots, BUFFER_CACHE_LINE_SIZE)` and calls `heap.alloc(region_layout)` exactly once.
- `BufferPoolInner` stores `region: Arc<Heap>`, `region_base`, `region_layout`, and `region_size`; no `Buffer` owns a `Vec`, `Slice`, `Box<[u8]>`, or separate payload allocation.
- If Hammer's encoded `BufferIndex` format could make the first allocation encode to zero, solve that in `BufferIndex` encoding or first-pool initialization only. Do not impose a per-pool slot-zero sentinel.
- `available` is initialized from `(0..slots).rev()` unless the first-pool/global-index-zero rule above requires skipping exactly one global buffer.
- `BUFFER_THREAD_CACHE_BATCH` stays `32`, matching the rounded refill chunk in `vlib_buffer_alloc_from_pool`.
- `BUFFER_THREAD_CACHE_HIGH_WATER` becomes `512`, matching `VLIB_BUFFER_POOL_PER_THREAD_CACHE_SZ` in local VPP.
- `BufferPoolInner::alloc_slot_with` and `alloc_slot_empty_fast` must pop from the current worker's `BufferThreadCache` first. They may touch arena `available` only when the cache is empty.
- `BufferPoolInner::refill_cache_batch` moves up to a 32-slot chunk from arena `available` into the current worker cache.
- `BufferPoolInner::push_cache_slot` fills the current worker cache up to 512 before `return_cache_batch` spills overflow to arena `available`, matching `vlib_buffer_pool_put`.
- `BufferPoolInner::buffer_raw_ptr(slot)` returns `region_base + slot * slot_stride` cast to `*mut Buffer`.
- `BufferPoolInner::data_raw_ptr(slot)` returns `region_base + slot * slot_stride + buffer_data_offset()`.
- `Buffer::current_ptr()` computes from the header pointer plus `buffer_data_offset()` plus signed `current_data`.
- `Buffer::prepend` accepts negative `current_data` down to `-DEFAULT_PRE_DATA_SIZE`.
- Add typed `DataPlaneError` variants needed by the layout implementation, including `BufferSlotOutOfBounds`, `BufferSlotOffsetOverflow`, and `BufferDataPointerOverflow`; do not use `CoreError::internal("...")` for these data-plane faults.
- `Drop for BufferPoolInner` unregisters the pool and deallocates `region_base` / `metadata_base` through `std::alloc::GlobalAlloc::dealloc(&*self.region, ...)` and `std::alloc::GlobalAlloc::dealloc(&*self.metadata_heap, ...)`.
- Per-slot metadata (`slots`, `available`, generation counters, cache lists) lives outside packet memory. Packet bytes live only in the heap-allocated arena region.

- [ ] **Step 4: Run green and commit**

Run:

```bash
cargo test -p hammer-adapter --test buffer_inline_layout -- --nocapture
cargo test -p hammer-adapter buffer -- --nocapture
cargo fmt --all -- --check
```

Expected: layout test passes, and existing buffer tests still pass.

Commit:

```bash
git add crates/hammer-adapter/src/buffer.rs crates/hammer-adapter/tests/buffer_inline_layout.rs
git commit -m "hammer-adapter(Feat): inline buffer slot storage"
```

---

### Task 4: Add Static No-Lock Memory Initialization Without New Public Managers

**Files:**
- Create: `crates/hammer-runtime/src/memory.rs`
- Modify: `crates/hammer-runtime/src/lib.rs`
- Modify: `crates/hammer-runtime/src/engine.rs`
- Test: `crates/hammer-runtime/tests/memory_static_init.rs`

**Interfaces:**
- Consumes: existing `DataPlaneBufferConfig`, `DataPlaneRuntimeConfig`, `BufferPoolArena::with_capacity_in`, `Heap`, `Engine`, and the existing `hammer-runtime::init` static `linkme` slices.
- Produces:
  - `memory_init` registered in the static init chain before `start_workers`.
  - private startup code that materializes fixed NUMA arenas directly into `DataPlaneRuntimeConfig`.
  - No heap/buffer init path uses `Mutex`, `RwLock`, `OnceLock`, `LazyLock`, `get_or_init`, `thread_local!`, `HashMap`, or `BTreeMap`.
  - No new public manager/table/config types.

- [ ] **Step 1: Write the failing static-init test**

Create `crates/hammer-runtime/tests/memory_static_init.rs`:

```rust
use hammer_runtime::init::{topological_order, INIT_FUNCTIONS};

#[test]
fn memory_init_sources_are_static_no_lock() {
    let runtime_memory = include_str!("../src/memory.rs");
    for forbidden in [
        "Mutex",
        "RwLock",
        "OnceLock",
        "LazyLock",
        "get_or_init",
        "thread_local!",
        "HashMap",
        "BTreeMap",
        "pub struct MemoryConfig",
        "pub struct MemoryMain",
        "pub struct StaticNumaTable",
        "pub struct HeapRegistry",
    ] {
        assert!(
            !runtime_memory.contains(forbidden),
            "crates/hammer-runtime/src/memory.rs must not use or expose {forbidden}"
        );
    }
}

#[test]
fn memory_init_is_registered_before_workers() {
    let memory = INIT_FUNCTIONS
        .iter()
        .find(|f| f.name == "memory_init")
        .expect("memory_init registration");
    assert!(
        memory.runs_before.contains(&"start_workers"),
        "memory_init must run before worker threads are spawned"
    );

    let order = topological_order(&INIT_FUNCTIONS).expect("init order");
    let memory_pos = order
        .iter()
        .position(|idx| INIT_FUNCTIONS[*idx].name == "memory_init")
        .expect("memory_init in order");
    let workers_pos = order
        .iter()
        .position(|idx| INIT_FUNCTIONS[*idx].name == "start_workers")
        .expect("start_workers in order");
    assert!(memory_pos < workers_pos);
}

```

- [ ] **Step 2: Run test red**

Run: `cargo test -p hammer-runtime --test memory_static_init -- --nocapture`

Expected before the fix: missing `hammer_runtime::memory`, no `memory_init` static registration, or existing memory code exposes `MemoryConfig` / `MemoryMain` / `StaticNumaTable`.

- [ ] **Step 3: Implement private fixed startup materialization**

Implementation requirements:

- Delete any public `crates/hammer-adapter/src/memory.rs` module if it only exists to expose memory manager/table types.
- Keep the fixed NUMA table representation private inside `crates/hammer-adapter/src/buffer.rs` or `crates/hammer-runtime/src/memory.rs`.
- `memory_init` builds `DataPlaneRuntimeConfig { buffers: DataPlaneBufferConfig { ... } }` directly from existing engine/startup config values.
- If this slice does not yet have full startup config plumbing, use constants private to `crates/hammer-runtime/src/memory.rs`; do not turn those constants into a public `MemoryConfig` type.
- `DataPlaneBufferConfig { buffer_arenas: Some(...) }` remains the only runtime construction point for prebuilt arenas; do not add public helper constructors such as `DataPlaneBufferConfig::arena_table()`.
- Do not export new public memory manager/table types through `hammer-adapter/src/lib.rs` or `hammer-runtime/src/lib.rs`.

- [ ] **Step 4: Register memory init statically**

Create `crates/hammer-runtime/src/memory.rs`. Keep arena-table construction behind the already-owned buffer module boundary; this snippet intentionally omits a new public helper constructor:

```rust
use std::sync::Arc;

use hammer_adapter::{
    BufferPoolArena, DataPlaneBufferConfig, DataPlaneRuntime, DataPlaneRuntimeConfig,
};
use hammer_component_macros::init_function;
use hammer_core::error::HammerResult;
use hammer_infra::heap::Heap;

use crate::engine::Engine;

const DEFAULT_BUFFER_SLOT_CAPACITY: usize = 2048;
const DEFAULT_BUFFER_SLOTS_PER_NUMA: usize = 4096;

#[init_function(name = "memory_init", runs_before = ["start_workers"])]
pub fn memory_init(engine: &mut Engine) -> HammerResult<()> {
    let heap = Arc::new(Heap::local());
    engine.runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig {
        buffers: DataPlaneBufferConfig {
            buffer_slot_capacity: DEFAULT_BUFFER_SLOT_CAPACITY,
            buffer_slots: DEFAULT_BUFFER_SLOTS_PER_NUMA,
            buffer_arenas: Some(private_single_numa_arena(
                engine.numa_node,
                BufferPoolArena::with_capacity_in(
                    DEFAULT_BUFFER_SLOT_CAPACITY,
                    DEFAULT_BUFFER_SLOTS_PER_NUMA,
                    heap,
                    engine.numa_node,
                ),
            )?),
            thread_index: engine.thread_index,
            active_numa_node: engine.numa_node,
            ..DataPlaneBufferConfig::default()
        },
    });
    Ok(())
}
```

Implementation requirements:

- Add `pub mod memory;` to `crates/hammer-runtime/src/lib.rs`.
- Do not store startup memory state in a global. The runtime owns the resulting arena/heap handles.
- Do not use `OnceLock`, `Mutex`, `RwLock`, `LazyLock`, or `thread_local!` in this module.
- Keep `memory_init` ordered before `start_workers` so every worker clone observes an already-materialized runtime.

- [ ] **Step 5: Run green and commit**

Run:

```bash
cargo test -p hammer-runtime --test memory_static_init -- --nocapture
cargo test -p hammer-adapter buffer -- --nocapture
cargo fmt --all -- --check
```

Expected: the static source-shape test passes, `memory_init` is registered before `start_workers`, and existing buffer tests still pass.

Commit:

```bash
git add crates/hammer-runtime/src/memory.rs crates/hammer-runtime/src/lib.rs \
        crates/hammer-runtime/src/engine.rs crates/hammer-runtime/tests/memory_static_init.rs
git commit -m "hammer-runtime(Feat): initialize memory from static no-lock tables"
```

---

### Task 5: Per-NUMA Arena Selection Without Heap Trait Objects

**Files:**
- Modify: `crates/hammer-adapter/src/buffer.rs`
- Modify: `crates/hammer-runtime/src/engine.rs`
- Modify: `crates/hammer-runtime/src/start_workers.rs`
- Test: `crates/hammer-adapter/tests/buffer_per_numa.rs`
- Test: `crates/hammer-runtime/tests/engine_numa_runtime.rs`

**Interfaces:**
- Consumes: `BufferPoolArena::with_capacity_in(..., Arc<Heap>, numa_node)` and `Engine.numa_node`.
- Produces: worker clones select a NUMA-local `BufferPool` while sharing node/runtime registries.

- [ ] **Step 1: Write the failing adapter NUMA test**

Create `crates/hammer-adapter/tests/buffer_per_numa.rs`:

```rust
use std::sync::Arc;

use hammer_adapter::buffer::{BufferPool, BufferPoolArena};
use hammer_infra::heap::Heap;

#[test]
fn arenas_keep_their_buffer_pool_numa_identity() {
    let a0 = BufferPoolArena::with_capacity_in(1024, 32, Arc::new(Heap::local()), 0);
    let a1 = BufferPoolArena::with_capacity_in(1024, 32, Arc::new(Heap::local()), 1);
    let p0 = BufferPool::with_arena(a0);
    let p1 = BufferPool::with_arena(a1);

    let i0 = p0.alloc_index().expect("numa0 alloc");
    let i1 = p1.alloc_index().expect("numa1 alloc");

    assert_eq!(p0.numa_node(), 0);
    assert_eq!(p1.numa_node(), 1);
    assert_ne!(p0.pool_id(), p1.pool_id());
    assert_ne!(p0.buffer_raw_ptr(i0.slot()), p1.buffer_raw_ptr(i1.slot()));
}
```

- [ ] **Step 2: Write the failing runtime NUMA clone test**

Create `crates/hammer-runtime/tests/engine_numa_runtime.rs`:

```rust
use hammer_adapter::DataPlaneRuntime;
use hammer_core::registry::RuntimeRegistry;
use hammer_runtime::engine::Engine;

#[test]
fn spawned_engine_uses_worker_numa_runtime_view() {
    let runtime = DataPlaneRuntime::new(runtime_config(2048, 64, &[0, 1]));
    let mut main = Engine::new(runtime, RuntimeRegistry::new());
    main.numa_node = 0;

    let worker = main.spawn_on_numa(3, 1);

    let main_index = main.runtime.alloc_index().expect("main alloc");
    let worker_index = worker.runtime.alloc_index().expect("worker alloc");

    assert_eq!(main.thread_index, 0);
    assert_eq!(worker.thread_index, 3);
    assert_eq!(worker.numa_node, 1);
    assert_ne!(main_index.pool_id(), worker_index.pool_id());
    assert_eq!(main.runtime.active_numa_node(), 0);
    assert_eq!(worker.runtime.active_numa_node(), 1);
}
```

- [ ] **Step 3: Run tests red**

Run:

```bash
cargo test -p hammer-adapter --test buffer_per_numa -- --nocapture
cargo test -p hammer-runtime --test engine_numa_runtime -- --nocapture
```

Expected before the fix: missing `with_capacity_in`, `numa_node`, config-driven NUMA construction, `spawn_on_numa`, `for_worker`, or `active_numa_node`.

- [ ] **Step 4: Implement NUMA-aware construction**

Add these exact APIs:

```rust
impl BufferPoolArena {
    pub fn with_capacity_in(slot_capacity: usize, slots: usize, heap: Arc<Heap>, numa_node: u32) -> Self;
    pub fn numa_node(&self) -> u32;
}

impl BufferPool {
    pub fn numa_node(&self) -> u32;
}

impl DataPlaneBuffers {
    pub fn new(config: DataPlaneBufferConfig) -> Self;
    pub fn try_buffers(&self) -> CoreResult<&BufferPool>;
    pub fn active_numa_node(&self) -> u32;
}

impl From<&DataPlaneBuffers> for DataPlaneBufferWorkerSeed;
impl From<DataPlaneBufferWorkerConfig> for DataPlaneBuffers;

impl DataPlaneRuntime {
    pub fn new(config: DataPlaneRuntimeConfig) -> Self;
    pub fn for_worker(&self, thread_index: u32, numa_node: u32) -> Self;
    pub fn active_numa_node(&self) -> u32;
}
```

Implementation requirements:

- `DataPlaneBuffers` stores a private fixed NUMA array of `BufferPool` plus `active_numa_node: u32` and `thread_index: u32`.
- `DataPlaneBuffers::try_buffers()` returns the active pool as `CoreResult<&BufferPool>`.
- `DataPlaneBufferWorkerSeed::from(&buffers)` captures the shared Buffer Arena handles and frame-pool sizing. `DataPlaneBuffers::from(DataPlaneBufferWorkerConfig { ... })` consumes that named source type to construct one worker-local buffer view; do not add tuple conversions or ad-hoc `*_clone`/`from_shared_*` constructors.
- `DataPlaneBufferConfig` is the only public construction surface. Tests and callers must build `DataPlaneRuntimeConfig { buffers: DataPlaneBufferConfig { ... } }`; do not keep capacity-only compatibility constructors.
- Add `Engine::spawn_on_numa(thread_index, numa_node)` and make worker startup use it so the runtime view is cloned after the worker NUMA node is known.
- `Engine::spawn(index)` remains a compatibility wrapper that calls `spawn_on_numa(index, self.numa_node)` only for callers that intentionally inherit the current engine's NUMA node.
- `start_workers.rs` computes each worker's NUMA node from worker placement/configuration (falling back to `numa::current_numa_node().unwrap_or(0)`) before cloning the runtime view. It must not clone a worker runtime with the main engine's NUMA node and patch the field afterward.
- The per-thread cache remains inside each `BufferPool` handle. Cloning for a worker creates a fresh `BufferPool` handle per active arena so cache vectors remain worker-local while arenas remain shared.

- [ ] **Step 5: Run green and commit**

Run:

```bash
cargo test -p hammer-adapter --test buffer_per_numa -- --nocapture
cargo test -p hammer-runtime --test engine_numa_runtime -- --nocapture
cargo test -p hammer-adapter buffer -- --nocapture
cargo fmt --all -- --check
```

Expected: all commands pass.

Commit:

```bash
git add crates/hammer-adapter/src/buffer.rs crates/hammer-runtime/src/engine.rs \
        crates/hammer-runtime/src/start_workers.rs \
        crates/hammer-adapter/tests/buffer_per_numa.rs \
        crates/hammer-runtime/tests/engine_numa_runtime.rs
git commit -m "hammer-runtime(Feat): select worker-local NUMA buffer arenas"
```

---

### Task 6: Make Typed `Frame<State>` Owners the Only Hot-Path Lifetime Mechanism

**Files:**
- Modify: `crates/hammer-adapter/src/buffer.rs`
- Test: `crates/hammer-adapter/tests/buffer_frame_guard.rs`

**Interfaces:**
- Consumes: `DataPlaneBuffers`, `FramePool`, `BufferPool`, and Hammer's RAII owner naming chosen in this plan.
- Produces:
  - `Frame<Next>` owns next-frame contents before `put_next_frame`.
  - `Frame<Pending>` owns a pending frame while a node is dispatching it.
  - `Drop` on either owner returns contained buffers and the frame slot to their pools.
  - Raw frame indices are scheduler-internal; callers do not receive an index they must later clean up.

- [ ] **Step 1: Write the failing guard test**

Create `crates/hammer-adapter/tests/buffer_frame_guard.rs`:

```rust
use hammer_adapter::{
    BufferFrame, DataPlaneBufferConfig, DataPlaneRuntime, DataPlaneRuntimeConfig,
    Frame, InternalNode, Next, Node, NodeId, NodeRegistration, NodeResult,
    NodeRuntimeData, Pending,
};

fn test_runtime(slot_capacity: usize, slots: usize) -> DataPlaneRuntime {
    DataPlaneRuntime::new(DataPlaneRuntimeConfig {
        buffers: DataPlaneBufferConfig {
            buffer_slot_capacity: slot_capacity,
            buffer_slots: slots,
            ..DataPlaneBufferConfig::default()
        },
    })
}

#[test]
fn dropping_frame_next_returns_buffers_and_frame_slot() {
    let runtime = test_runtime(256, 128);
    assert_eq!(runtime.in_use_buffers(), 0);
    assert_eq!(runtime.frames_in_use(), 0);

    {
        let drop_node = runtime.nodes().register_internal(DropNode);
        let mut frame = runtime
            .buffers()
            .get_next_frame(drop_node)
            .expect("frame<next>");
        for _ in 0..8 {
            let index = runtime.alloc_index().expect("buffer");
            frame.push_index(index).expect("push index");
        }
        assert_eq!(frame.len(), 8);
        assert_eq!(runtime.in_use_buffers(), 8);
        assert_eq!(runtime.frames_in_use(), 1);
    }

    assert_eq!(runtime.in_use_buffers(), 0);
    assert_eq!(runtime.frames_in_use(), 0);
}

#[test]
fn put_next_frame_transfers_cleanup_to_pending_owner() {
    let runtime = test_runtime(256, 128);
    let drop_node = runtime.nodes().register_internal(DropNode);
    let mut frame = runtime
        .buffers()
        .get_next_frame(drop_node)
        .expect("frame<next>");
    let index = runtime.alloc_index().expect("buffer");
    frame.push_index(index).expect("push index");

    runtime.put_next_frame(frame).expect("put next frame");
    assert_eq!(runtime.in_use_buffers(), 1);
    assert_eq!(runtime.frames_in_use(), 1);

    assert_eq!(runtime.run_ready_nodes().expect("run drop node"), 1);
    assert_eq!(runtime.in_use_buffers(), 0);
    assert_eq!(runtime.frames_in_use(), 0);
}

#[test]
fn pending_frame_converts_to_next_with_into_trait() {
    let runtime = test_runtime(256, 128);
    let sink = runtime.nodes().register_internal(DropNode);
    let forward = runtime.nodes().register_internal(ForwardPendingNode { target: sink });
    let mut frame = runtime
        .buffers()
        .get_next_frame(forward)
        .expect("frame<next>");
    let index = runtime.alloc_index().expect("buffer");
    frame.push_index(index).expect("push index");

    runtime.put_next_frame(frame).expect("put next frame");
    assert_eq!(runtime.run_ready_nodes().expect("run forwarded frame"), 2);
    assert_eq!(runtime.in_use_buffers(), 0);
    assert_eq!(runtime.frames_in_use(), 0);
}
```

- [ ] **Step 2: Run test red**

Run: `cargo test -p hammer-adapter --test buffer_frame_guard -- --nocapture`

Expected before the fix: dropping `Frame<Next>` does not return resources to the pools, or `put_next_frame` is missing.

- [ ] **Step 3: Implement RAII ownership**

Make `Next` and `Pending` the concrete state bodies. Do not add marker states, `PhantomData`, `ManuallyDrop`, `FrameStorage`, or `FrameOwner`:

```rust
pub struct Frame<State> {
    state: State,
}

pub struct Next {
    owner: DataPlaneBuffers,
    index: FrameIndex,
    next: NodeId,
    frame: Option<BufferFrame>,
}

pub struct Pending {
    owner: DataPlaneBuffers,
    index: FrameIndex,
    frame: Option<BufferFrame>,
}

impl Frame<Next> {
    pub fn index(&self) -> FrameIndex {
        self.state.index
    }
}

impl Drop for Next {
    fn drop(&mut self) {
        if let Some(frame) = self.frame.take() {
            self.owner.drop_owned_frame(self.index, frame);
        }
    }
}

impl Drop for Pending {
    fn drop(&mut self) {
        if let Some(frame) = self.frame.take() {
            self.owner.drop_owned_frame(self.index, frame);
        }
    }
}
```

Implementation requirements:

- `Frame<State>` does not implement `Drop`; `Drop` lives on the concrete state bodies `Next` and `Pending`.
- `DataPlaneError` uses typed variants for ownership faults needed by the private implementation, such as `FrameSlotCheckedOut`. Do not report these with ad hoc strings.
- Do not add `FrameStateAccess`, `FrameStorage`, `FrameOwner`, or any generic owner-access helper. Implement concrete `impl Frame<Next>` and `impl Frame<Pending>` methods directly.
- `Frame<Next>` and `Frame<Pending>` may implement `Deref` / `DerefMut` to `BufferFrame` for node ergonomics; do not add separate public `frame()` / `frame_mut()` helpers.
- Do not add public conversion traits or inherent conversion helpers such as `Frame<Pending>::into_next(next)` or `Frame<Next>::into_pending()`. The only `Next -> Pending` move is inline inside `DataPlaneRuntime::put_next_frame` / empty-frame scheduling.
- `DataPlaneBuffers::get_next_frame(next)` fills `state: Next { owner: self.clone(), index, next, frame: Some(frame) }`.
- `DataPlaneRuntime::put_next_frame(frame)` consumes `Frame<Next>`, reads the private `next` field, builds the scheduler-owned `Frame<Pending>` inside `buffer.rs`, schedules it, and returns `CoreResult<()>`. It must not take a separate `NodeId`.
- There is no public pending-queue extraction API; pending frames are scheduler-owned until normal node dispatch receives them.
- There is no `Frame<Pending> -> Frame<Next>` transition. During node processing, forwarding to another arc uses a fresh `Frame<Next>` from `get_next_frame(next)` and submits it with `put_next_frame`.
- Do not implement `TryFrom` / `From` conversions between `Frame<Next>` and `Frame<Pending>` unless a later design explicitly approves a public conversion surface.
- Remove the public scheduled-frame lifetime escape hatch; dropping the owner or consuming it into the scheduler is the only lifetime path.
- Do not add public or crate-visible packet/frame `free_*`, `release_*`, `reclaim_*`, or `recycle_*` methods. If `buffer.rs` needs a helper to keep `Drop` readable, use one private `drop_owned_frame` function and call it only from `Drop for Next` / `Drop for Pending`.

- [ ] **Step 4: Run green and commit**

Run:

```bash
cargo test -p hammer-adapter --test buffer_frame_guard -- --nocapture
cargo test -p hammer-adapter buffer -- --nocapture
cargo fmt --all -- --check
```

Expected: guard tests pass and existing frame-pool tests pass.

Commit:

```bash
git add crates/hammer-adapter/src/buffer.rs crates/hammer-adapter/tests/buffer_frame_guard.rs
git commit -m "hammer-adapter(Feat): typed frame states clean up on Drop"
```

---

### Task 7: Migrate Runtime and Service Hot Paths To Drop-Owned Cleanup

**Files:**
- Modify: `crates/hammer-adapter/src/node.rs`
- Modify: `crates/hammer-adapter/src/node/next.rs`
- Modify: `crates/hammer-adapter/src/handoff.rs`
- Modify: `crates/hammer-runtime/src/spawn.rs`
- Modify: `crates/hammer-service/src/**/*.rs`
- Test: `crates/hammer-service/tests/frame_owner_cleanup_hot_path.rs`

**Interfaces:**
- Consumes: `Frame<Next>`, `Frame<Pending>`, and `DataPlaneRuntime::put_next_frame`.
- Produces:
  - hot-path ownership is frame-scoped; packet/frame cleanup is only reachable through `buffer.rs` owner `Drop` implementations.

- [ ] **Step 1: Write the source-level failing test**

Create `crates/hammer-service/tests/frame_owner_cleanup_hot_path.rs`:

```rust
use std::fs;
use std::path::{Path, PathBuf};

fn forbidden_lifetime_tokens() -> &'static [&'static str] {
    &[
        ".free(",
        ".free_index(",
        ".free_frame(",
        ".free_frame_index(",
        "free_index(",
        "free_frame(",
        "free_frame_index(",
        "release_pooled",
        ".release_",
        ".reclaim_",
        ".recycle_",
        "pub fn drop_owned_frame",
        "NodeNextFrames",
        "submit_frame(",
        "NodeResult::next_frame",
        "NodeResult::next_current",
        "pub enum NextFrame",
        "NextFrame::",
        "Current(NodeId)",
        "FrameOwnerConsumed",
        "FrameStateAccess",
        "_: NodeId",
        "_node: NodeId",
        "_next: NodeId",
    ]
}

fn forbidden_public_surface_tokens() -> &'static [&'static str] {
    &[
        concat!("pub type Node", "Pending", "ProcessFn"),
        "pub struct MemoryConfig",
        "pub struct MemoryMain",
        "pub struct StaticNumaTable",
        "pub numa_nodes:",
    ]
}

#[test]
fn runtime_service_and_node_hot_paths_use_frame_owner_cleanup() {
    let root = workspace_root();
    let tokens = forbidden_lifetime_tokens();
    let mut failures = String::new();
    for dir in [
        "crates/hammer-adapter/src",
        "crates/hammer-runtime/src",
        "crates/hammer-service/src",
    ] {
        visit_rust_files(&root.join(dir), &mut |path| {
            if allowed_pool_internal(path) {
                return;
            }
            let src = fs::read_to_string(path).expect("read source");
            for token in tokens {
                if src.contains(*token) {
                    failures.push_str(&format!("{} contains {}\n", path.display(), token));
                }
            }
        });
    }
    for dir in ["crates/hammer-adapter/src", "crates/hammer-runtime/src"] {
        visit_rust_files(&root.join(dir), &mut |path| {
            let src = fs::read_to_string(path).expect("read source");
            for token in forbidden_public_surface_tokens() {
                if src.contains(*token) {
                    failures.push_str(&format!("{} exposes {}\n", path.display(), token));
                }
            }
        });
    }
    let buffer_src = fs::read_to_string(root.join("crates/hammer-adapter/src/buffer.rs"))
        .expect("read buffer.rs");
    for forbidden in [
        "pub fn drop_owned_frame",
        "pub(crate) fn drop_owned_frame",
        "trait FrameStateAccess",
        "unreachable!()",
        "pub fn free_",
        "pub(crate) fn free_",
        "pub fn release_",
        "pub(crate) fn release_",
        "pub fn reclaim_",
        "pub(crate) fn reclaim_",
        "pub fn recycle_",
        "pub(crate) fn recycle_",
    ] {
        assert!(
            !buffer_src.contains(forbidden),
            "buffer.rs must not expose lifetime helper `{forbidden}`"
        );
    }
    assert!(failures.is_empty(), "{failures}");
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crates")
        .parent()
        .expect("workspace")
        .to_path_buf()
}

fn visit_rust_files(dir: &Path, f: &mut impl FnMut(&Path)) {
    for entry in fs::read_dir(dir).expect("read dir") {
        let entry = entry.expect("dir entry");
        let path = entry.path();
        if path.is_dir() {
            visit_rust_files(&path, f);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            f(&path);
        }
    }
}

fn allowed_pool_internal(path: &Path) -> bool {
    path.ends_with("crates/hammer-adapter/src/buffer.rs")
}
```

- [ ] **Step 2: Run test red**

Run: `cargo test -p hammer-service --test frame_owner_cleanup_hot_path -- --nocapture`

Expected before the fix: failures listing current direct lifetime calls in adapter node code, runtime spawn tests/helpers, service session/tun/tcp/net paths.

- [ ] **Step 3: Migrate ownership paths**

Apply these replacement rules:

- Delete every caller-side `free_index(...)`, `free_frame(...)`, `free_frame_index(...)`, `release_pooled_*`, and equivalent lifetime-call branch from adapter/runtime/service hot paths. Do not leave the old call in an `if false`, helper wrapper, compatibility shim, dead branch, or commented code.
- Delete every caller-side `NodeNextFrames`, `submit_frame(...)`, `NodeResult::next_frame(...)`, `NodeResult::next_current(...)`, `NextFrame::{...}`, and `Current(NodeId)` forwarding carrier. Do not replace these with a renamed result carrier or private staging helper.
- Delete migrated API compatibility parameters such as `_: NodeId`. If a `Frame<Next>` already owns the selected next node, the receiving function must not also accept a node argument.
- Delete pending-process callback aliases, public memory manager/table types, and public NUMA table fields.
- Where code allocates a frame, processes packet indices, and then calls a legacy lifetime helper, keep the `Frame<Next>` in scope and let `Drop` own the path where the frame is not passed to `put_next_frame`.
- Where code sends work to a next node, call `runtime.put_next_frame(frame)` and do not use the frame afterward.
- Where code currently handles individual indices during packet-drop paths, make those indices belong to the current `Frame<Next>`; do not call runtime lifetime helpers.
- Where code allocates a single reply/output buffer, get a `Frame<Next>` for the selected next arc first, push the buffer into it, and either pass the frame to `put_next_frame` or let it drop.
- Adapter internals may keep one private `drop_owned_frame` helper in `buffer.rs`; no other source file may call it, and it must not be `pub` or `pub(crate)`.

Deletion requirements:

- Caller code that existed only to sequence explicit frees must be removed, not renamed.
- If a function's only purpose was explicit packet/frame release from non-`buffer.rs` code, delete the function and route ownership through `Frame<State>` instead.
- If removing a caller makes a local variable, branch, return value, or test assertion obsolete, delete that obsolete code in the same task.

The resulting hot-path shape must look like this:

```rust
let mut frame = runtime.buffers().get_next_frame(next)?;
let index = runtime.alloc_index_with_bytes(packet_bytes)?;
frame.push_index(index)?;

if should_forward {
    runtime.put_next_frame(frame)?;
    return Ok(());
}

// No lifetime call here. The frame drops and owns `index`.
Ok(())
```

- [ ] **Step 4: Make owner `Drop` the only public lifetime path**

In `crates/hammer-adapter/src/buffer.rs`, remove every public packet/frame lifetime method matched by `forbidden_lifetime_tokens()` from `DataPlaneBuffers`, `DataPlaneRuntime`, and `BufferPool`. Do not replace them with differently named public helpers.

Replacement rules:

- Internal buffer-pool code may keep one private helper named `drop_owned_frame`.
- Only `Drop for Next` and `Drop for Pending` may call `drop_owned_frame`.
- External tests must use scoped owners and `drop(owner)`, not direct lifetime calls.
- Task 7 is not complete if any caller outside `buffer.rs` still mentions deleted lifetime APIs or result-carrier forwarding APIs, even if the API itself has already been removed.

- [ ] **Step 5: Run green and commit**

Run:

```bash
cargo test -p hammer-service --test frame_owner_cleanup_hot_path -- --nocapture
cargo check -p hammer-service
cargo test -p hammer-adapter --test buffer_frame_guard -- --nocapture
cargo fmt --all -- --check
```

Expected: no source-level hot-path lifetime calls outside `buffer.rs`, no result-carrier forwarding APIs remain, focused adapter/service checks pass, and formatting is clean.

Commit:

```bash
git add crates/hammer-adapter/src crates/hammer-runtime/src crates/hammer-service/src \
        crates/hammer-service/tests/frame_owner_cleanup_hot_path.rs
git commit -m "hammer-service(Refactor): use typed frame ownership on hot paths"
```

---

### Task 8: Final Audit and Focused Verification

**Files:**
- Modify: `docs/superpowers/plans/2026-07-01-vpp-memory-management.md` if execution notes need checkbox updates only.
- No production code changes unless a preceding test exposes a missed direct lifetime call.

**Interfaces:**
- Consumes: Tasks 1-7.
- Produces: verified focused test/clippy state and source-level invariants.

- [ ] **Step 1: Run source-shape audits**

Run:

```bash
rg -n "pub trait Heap|HeapLocal|HeapSvm|H: Heap|dyn GlobalAlloc|#\[global_allocator\]|std::alloc::Allocator|pub unsafe fn dealloc|pub fn numa_node|pub fn free|pub\\(crate\\) fn free|pub fn freelist_len|pub fn release_|pub\\(crate\\) fn release_|pub fn reclaim_|pub\\(crate\\) fn reclaim_|pub fn recycle_|pub\\(crate\\) fn recycle_|fn local_alloc|fn local_dealloc|fn local_numa|fn svm_alloc|fn svm_dealloc|fn svm_numa|fn alloc_region|fn page_size|HeapRegistry" crates/hammer-infra/src crates/hammer-adapter/src
rg -n "ReturnedSpan|returned_spans|returned_nodes|return_span|bump_depth|std::sync::Mutex|std::vec::Vec|hammer_infra::vec::Vec|Vec<|panic!|unreachable!" crates/hammer-infra/src/svm_region.rs crates/hammer-infra/src/heap.rs
rg -n "NodeNextFrames|NodeResult::error|NodeResult::next_frame|NodeResult::next_current|pub enum NextFrame|NextFrame::|Current\\(NodeId\\)|submit_frame|pub fn append_to_frame|pub fn enqueue_to_next_frames|unsafe impl Send for .*Frame|Frame<Pending>.*TunPendingTx|SegmentAllocOwner|FrameOwnerConsumed|FrameStateAccess|pub type Node.*Pending.*ProcessFn|pub struct MemoryConfig|pub struct MemoryMain|pub struct StaticNumaTable|pub numa_nodes:|_: NodeId|_node: NodeId|_next: NodeId" crates/hammer-adapter/src crates/hammer-runtime/src crates/hammer-service/src crates/hammer-infra/src
cargo test -p hammer-service --test frame_owner_cleanup_hot_path -- --nocapture
```

Expected:

- The three `rg` commands print no matches.
- The test command passes, proving hot-path code reaches packet/frame cleanup only through owner `Drop`.

- [ ] **Step 2: Run focused tests**

Run:

```bash
cargo test -p hammer-infra --test heap -- --nocapture
cargo test -p hammer-infra --test pool_heap -- --nocapture
cargo test -p hammer-infra --test slice_map_heap -- --nocapture
cargo test -p hammer-adapter --test buffer_inline_layout -- --nocapture
cargo test -p hammer-adapter --test buffer_per_numa -- --nocapture
cargo test -p hammer-adapter --test buffer_frame_guard -- --nocapture
cargo test -p hammer-service --test frame_owner_cleanup_hot_path -- --nocapture
```

Expected: every command passes.

- [ ] **Step 3: Run crate-level verification**

Run:

```bash
cargo test -p hammer-infra --test heap -- --nocapture
cargo test -p hammer-infra --test pool_heap -- --nocapture
cargo test -p hammer-infra --test slice_map_heap -- --nocapture
cargo test -p hammer-adapter --test buffer_inline_layout -- --nocapture
cargo test -p hammer-adapter --test buffer_per_numa -- --nocapture
cargo test -p hammer-adapter --test buffer_frame_guard -- --nocapture
cargo test -p hammer-service --test frame_owner_cleanup_hot_path -- --nocapture
cargo fmt --all -- --check
cargo clippy -p hammer-infra --all-targets
cargo clippy -p hammer-adapter --all-targets
cargo clippy -p hammer-runtime --all-targets
cargo clippy -p hammer-service --all-targets
```

Expected: every command passes.

- [ ] **Step 4: Commit audit updates**

If the only changes are checkbox updates in this plan:

```bash
git add docs/superpowers/plans/2026-07-01-vpp-memory-management.md
git commit -m "docs(Plan): finalize VPP memory management execution checklist"
```

If code changed because the audit exposed a missed call site, commit with the affected crate scope instead of `docs`.

---

## Self-Review

**Spec coverage:**

- Concrete heap, no trait-object heap design: Tasks 1-2.
- Static no-lock initialization: Task 4.
- VPP-style owner-created SVM data heap region and NUMA buffer-pool lookup: Tasks 1 and 5. Full VPP attach-side SVM allocation (`ssvm_mem_alloc` from an attached process), SVM region/private heap modeling, and per-NUMA heap identity are explicitly out of scope and must not be implied by this plan.
- Infra containers use the heap and deallocate through the same handle: Task 2.
- Buffer header + pre-data + payload live in one contiguous slot region: Task 3.
- Signed `current_data` and 128-byte pre-data headroom: Task 3.
- Per-worker cache remains the hot allocation/return path: Tasks 3, 5, and 6 preserve `BufferThreadCache`; Task 7 makes external cleanup Drop-owned.
- Data-plane hot path no longer calls packet/frame lifetime helpers directly: Tasks 6-8.
- Runtime/service adaptation included: Tasks 5 and 7.

**Placeholder scan:**

- No unresolved placeholder text is present.
- Every new public API referenced by a later task is listed in the Public API Approval Ledger.
- Every test command names an exact crate and test target.

**Type consistency:**

- Heap-bearing APIs consistently use `Arc<Heap>`.
- Buffer layout APIs consistently use `slot_capacity`, `slots`, `slot_stride`, `buffer_data_offset`, `buffer_raw_ptr`, and `data_raw_ptr`.
- Frame ownership consistently uses `Frame<Next>`, `Frame<Pending>`, `Drop`, and `put_next_frame`; public `TryFrom` / `From` frame-state conversions are not part of this surface.
- Graph dispatch consistently uses Hammer RAII next-frame ownership inspired by VPP get/put semantics: no `NodeNextFrames`, no `NextFrame` result carrier, no `Current(NodeId)`, no `NodeResult::next_frame`, no `submit_frame`, and no compatibility `_ : NodeId` parameters.

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-07-01-vpp-memory-management.md`. Two execution options:

**1. Subagent-Driven (recommended)** - dispatch a fresh subagent per task, review between tasks, fast iteration.

**2. Inline Execution** - execute tasks in this session using executing-plans, batch execution with checkpoints.
