# VPP-Style Buffer, Physmem, and Frame Ownership

Status: accepted

Date: 2026-09-07

Hammer will replace its generational packet-pool identity, guarded Buffer
Arena, and growable Buffer Frame with VPP's process-wide Buffer Index,
Physmem-backed Buffer Pools, `vlib_buffer_t` layout, and fixed Frame lifecycle.
The data plane keeps raw compact indices and VPP enqueue/free semantics. The
free family returns Buffer slots to Pools. It never deallocates their backing
memory; Rust ownership and `Drop` govern Physmem mappings and ordinary Rust
allocations when their owning values reach the end of their lifetime. This is
a design record only and does not claim that the current Rust implementation
already satisfies the decision.

## Context

The current checkout uses a 16-byte `Index { pool_id, slot, generation }`, an
`Arc<RwSpinlock<_>>` Buffer Arena, `BufferRef`/`BufferRefMut` lock guards, and a
`BufferFrame` backed by `Vec<Index>`. Buffer slots currently contain two header
cache lines followed by separately addressed pre-data and packet storage. The
current positive `headroom` value is an offset within packet storage rather
than VPP's aligned boundary immediately before `pre_data`.

Those choices conflict with the VPP graph and Buffer contracts in several
ways. A VPP Frame carries contiguous `u32` Buffer Indices; ordinary forwarding
copies those indices into a Next Frame without removing elements from the
input Frame. Buffer lookup is one base-plus-shift operation. Buffer Pool
allocation uses per-thread caches and a central free list over slots carved
from a Physmem mapping. The Buffer header, inline pre-data, and trailing packet
data form one allocation with compile-time layout.

Ordinary packet discard follows VPP's Drop Node. The Drop Node and retaining
domain lifecycles use the VPP-shaped Buffer free family to return slots to
Pools without placing a different element type in a Frame. Destroying the
backing mappings remains the responsibility of their Rust owners' `Drop`
implementations.

## Decision

### Process-wide Buffer identity

The packet Buffer Index is a raw `u32`. The existing generic packet
`Index { pool_id, slot, generation }` type is removed. Zero is permanently
invalid, and copying an Index does not change any reference count or transfer
release responsibility.

All Buffer Pool mappings lie within one address span representable by a
32-bit index at 64-byte granularity. A Buffer address is derived as:

```text
buffer_mem_start + (buffer_index << 6)
```

The span may contain gaps and is limited to 256 GiB. Pool membership is read
from the Buffer's first cache line rather than encoded in the Index. At most
255 Buffer Pools exist; pool index `0xff` is invalid.

There is no generation check on packet Buffer lookup. A stale, foreign,
released, or concurrently owned Index is a programmer invariant violation.
The graph, handoff, and long-lived domain-owner lifecycles must prevent stale
copies from surviving slot reuse.

### Physmem and Buffer Pools

Physmem is the only backing-memory source for packet Buffer storage. A Physmem
mapping owns one shared page-based region and its NUMA/page placement. A Buffer
Pool references that mapping, carves Buffer slots from it, and owns individual
slot allocation and recycling. No `BufferAllocator` or other allocation
authority exists beside this Physmem-to-Buffer-Pool relationship, and Buffer
storage never falls back to the Hammer Main Heap.

Pool creation follows `vlib_buffer_pool_create`:

- align the first candidate to the Buffer allocation stride;
- sacrifice at most one candidate when required to keep Buffer Index zero
  invalid;
- skip every candidate whose complete allocation crosses a backing page
  boundary;
- initialize each accepted Buffer from the pool's first-cache-line template;
- retain the pool's central free-index storage and one cache per Data Worker.

Each pool has one data capacity, allocation stride, NUMA placement, central
free list, per-worker caches, and Buffer template. The template has
`ref_count = 1` and that pool's `buffer_pool_index`. Allocation restores the
template but does not clear the second cache line, optional trajectory,
pre-data, or packet data. Final release restores the template before returning
the Index to the owning pool.

### Buffer layout

The Hammer Buffer follows vendored `vlib_buffer_t` field order and alignment:

1. a 64-byte first-cache-line template overlay containing `current_data`,
   `current_length`, `flags`, `flow_id`, `ref_count`, `buffer_pool_index`,
   `error`, `next_buffer`, the `current_config_index`/`punt_reason` overlay,
   and `opaque[10]`;
2. the 64-byte-aligned second half containing `trace_handle`,
   `total_length_not_including_first_buffer`, and `opaque2[14]`;
3. the optional dedicated 64-byte trajectory area, selected by one
   compile-time configuration shared by the daemon and all plugins;
4. the 64-byte-aligned `headroom` marker;
5. inline `pre_data[BUFFER_PRE_DATA_SIZE]`;
6. trailing packet `data[]` in the same Buffer Pool allocation.

`headroom` names the aligned start of `pre_data`; it is not a positive offset
inside packet data. `current_data` is a signed displacement from `data[0]`,
starts at zero, and may move backward into `pre_data`. Available prepend space
is `BUFFER_PRE_DATA_SIZE + current_data`. `BUFFER_PRE_DATA_SIZE` is a
cache-line multiple selected at compile time. Optional trajectory changes the
Buffer header size in the same build-wide manner as VPP.

Hammer generates that compile-time configuration in `hammer-core/build.rs`,
corresponding to VPP's generated `vlib/config.h` and global trajectory compile
definition. `HAMMER_BUFFER_PRE_DATA_SIZE` defaults to 128 and must be a multiple
of 64 representable by the signed `current_data` range.
`HAMMER_BUFFER_TRACE_TRAJECTORY` accepts only `0` or `1` and defaults to `0`;
the build script emits a private Rust `cfg` that includes the 64-byte
`trajectory_nb`/`trajectory_trace[31]` area only when enabled. It also generates
the public `BUFFER_PRE_DATA_SIZE`, `BUFFER_TRACE_TRAJECTORY`,
`BUFFER_TRACE_TRAJECTORY_SIZE`, and `BUFFER_HEADER_SIZE` constants used by the
layout and its consumers. These are build inputs, not startup TOML settings.

Cargo feature unification is not accepted as the cross-DSO layout contract
because a plugin may be built in another Cargo invocation. Each plugin's
`PluginMetadata` therefore records `buffer_pre_data_size`,
`buffer_trajectory_size`, `buffer_size`, and `buffer_alignment` as fixed-width
integers populated from its compiled `hammer-core`. Immediately after loading
the ABI-stable plugin root, and before dereferencing or collecting its opaque
`RegistrationImage`, `PluginMain` compares those fields with the host values.
A mismatch returns `PluginError::BufferLayoutMismatch` and publishes none of
the transaction's libraries, metadata, or registrations. Opened libraries
retain the existing process-lifetime mapping policy. Matching package semver
does not bypass this layout check.

Only generic Buffer flags occupy the core flag bits. The current private
capacity and clean-state flag bits are removed. Data capacity belongs to the
Buffer Pool, and validity of second-half or opaque fields follows the public
first-cache-line flags and the operation that owns those fields.

### Buffer access and chains

`BUFFER_MAIN` is the independent process-global `BufferMain` authority. It is
initialized once before any Data Worker starts and remains valid for the
process lifetime. It is not a field of, or owned by, `GlobalMain`.
`DataPlaneMain` reaches the same global value and uses its own Worker index to
select per-Worker Pool caches. `buffer_mem_start`, Buffer address ranges, the
Pool table, and default NUMA Pool selection are immutable after publication.

Construction and publication use one safe startup API:

```rust
impl BufferMain {
    pub fn new(
        data_size: usize,
        buffers_per_numa: usize,
        numa_nodes: &[u32],
        worker_count: usize,
        page_size: PageSize,
    ) -> DataPlaneResult<Self>;
    pub fn publish(self) -> &'static Self;
    pub fn global() -> &'static Self;
}
```

`new` builds an unpublished value so huge-page fallback and startup rollback
drop every partially created Physmem mapping normally. Its recoverable failures
are the typed `DataPlaneError` variants `BufferAllocationSizeOverflow {
data_size }`, `BufferAllocationExceedsPage { allocation_size, page_size }`,
`BufferPoolCountExceeded { requested, maximum }`, `BufferMemorySpanExceeded {
bytes, maximum }`, `BufferPoolsUnavailable`, and `BufferPoolMapping { numa_node,
source }`. `publish` panics on a second publication, and `global` panics before
publication, because both violate startup ordering rather than describe a
recoverable runtime condition. Frame size-class allocation and ordinary Buffer
allocation use the Main Heap fatal-allocation policy and short counts,
respectively; neither adds a per-allocation `Result`.

A raw Index does not independently produce a Rust reference. Node and retained
domain code borrow a Buffer while holding the owning Worker's
`DataPlaneMain`; mutable Buffer access requires `&mut DataPlaneMain`. The
Index-to-pointer conversion is one private unsafe boundary in `hammer-core`.
Lookup is infallible after the ownership invariant has been established.

The public direct-borrow API is:

```rust
impl DataPlaneMain {
    pub fn buffer(&self, index: u32) -> &Buffer;
    pub fn buffer_mut(&mut self, index: u32) -> &mut Buffer;
}
```

Both methods are safe Rust and return direct borrows rather than guards or
`Result`. The mutable borrow is tied to the exclusive `DataPlaneMain` borrow;
it must end before the caller gets or puts a Next Frame. Node code therefore
finishes its per-Buffer access before the enqueue phase. Invalid, stale,
foreign, released, or non-exclusively-owned Indices violate the caller's graph
ownership invariant and trigger a local assertion or panic.

Single-segment data-window operations follow the corresponding vendored VPP
helpers while exposing only safe Rust slices:

```rust
impl Buffer {
    pub fn advance(&mut self, displacement: isize);
    pub fn reset(&mut self);
    pub fn space_left_at_end(&self) -> usize;
    pub fn put_uninit(&mut self, len: u16) -> &mut [u8];
    pub fn push_uninit(&mut self, len: u8) -> &mut [u8];
    pub fn make_headroom(&mut self, len: u8) -> &mut [u8];
    pub fn pull(&mut self, len: u8) -> Option<&[u8]>;
}
```

`advance` applies a signed displacement to `current_data` and the inverse
change to `current_length`; `reset` restores `current_data` to zero and keeps
only bytes at or after `data[0]`. `space_left_at_end` and `put_uninit` obtain
the segment data capacity from the Pool selected by `buffer_pool_index` in the
process `BUFFER_MAIN`. `put_uninit` returns the appended uninitialized region
and increases `current_length` before returning. `push_uninit` moves backward,
increases `current_length`, and returns the newly exposed prefix.
`make_headroom` moves `current_data` forward without changing
`current_length`, matching `vlib_buffer_make_headroom`, and returns the writable
slice from the new current position through the end of that Pool's data area;
the returned length equals the post-move `space_left_at_end()`. `pull` returns
the removed prefix and advances only when `len <= current_length`; otherwise it
returns `None` without mutation. Integer overflow, insufficient pre-data or
tail capacity, and an invalid resulting current window are programmer
invariant violations and panic. None of these methods returns a raw pointer or
a recoverable packet-path `Result`.

Chain data append operations are safe `DataPlaneMain` methods:

```rust
impl DataPlaneMain {
    pub fn buffer_add_data(&mut self, head: &mut u32, data: &[u8]) -> usize;
    pub fn buffer_chain_append_data(
        &mut self,
        first: u32,
        last: u32,
        data: &[u8],
    ) -> usize;
    pub fn buffer_chain_append_data_with_alloc(
        &mut self,
        first: u32,
        last: &mut u32,
        data: &[u8],
    ) -> usize;
}
```

`buffer_add_data` follows an existing chain to its last segment and allocates
additional segments from the calling Worker's default Pool when required. If
`head` contains the VPP invalid sentinel `u32::MAX`, it first allocates an empty
head and immediately writes the new Index back through `head` so the caller
holds its release obligation even if a later allocation fails. It clears
`TOTAL_LENGTH_VALID`, matching `vlib_buffer_add_data`.

`buffer_chain_append_data` copies only what fits after `last`, allocates
nothing, and updates `first.total_length_not_including_first_buffer` when
`first != last`. `buffer_chain_append_data_with_alloc` continues by allocating
segments from `first.buffer_pool_index`; after each successful link it
immediately writes the new last Index through `last`. All three return the
number of bytes appended. A return smaller than `data.len()` means Pool
pressure stopped the operation; the already-appended prefix and every newly
linked segment remain in the chain. The caller retains one release obligation
for the resulting head chain. Invalid first/last relationships, shared mutable
segments, length overflow, and malformed chains are programmer invariant
violations and panic rather than partial-success conditions.

Batch allocation follows the complete vendored VPP family. The destination is
caller-owned initialized `u32` storage, and only the returned prefix is
overwritten with newly allocated Indices. Rust slices carry the requested
count and ring size:

```rust
impl DataPlaneMain {
    pub fn buffer_alloc(&mut self, indices: &mut [u32]) -> usize;
    pub fn buffer_alloc_from_pool(
        &mut self,
        indices: &mut [u32],
        pool_index: u8,
    ) -> usize;
    pub fn buffer_alloc_on_numa(
        &mut self,
        indices: &mut [u32],
        numa_node: u32,
    ) -> usize;
    pub fn buffer_alloc_to_ring(
        &mut self,
        ring: &mut [u32],
        start: usize,
        count: usize,
    ) -> usize;
    pub fn buffer_alloc_to_ring_from_pool(
        &mut self,
        ring: &mut [u32],
        start: usize,
        count: usize,
        pool_index: u8,
    ) -> usize;
}
```

These methods write accepted Buffer Indices directly into caller-owned
storage, normally a Frame's writable vector suffix, and return a short count on
Pool pressure. Chain extension allocates directly into the existing chain. A
successful public allocation does not return an unowned raw Index, and no
ordinary public single-Index allocation API is retained. Out-of-range NUMA or
Pool indices and invalid ring ranges are caller invariant violations and panic;
Pool pressure returns zero or a short count.

The safe chain ownership APIs are:

```rust
impl DataPlaneMain {
    pub fn buffer_chain_init(&mut self, first: u32);
    pub fn buffer_chain_buffer(&mut self, last: u32, next: u32) -> u32;
    pub fn buffer_attach_clone(&mut self, head: u32, tail: u32);
}
```

`buffer_chain_buffer` transfers the appended Buffer's release responsibility
to the chain and returns the appended Index for continued construction.
`buffer_attach_clone` requires an unchained head and, like vendored
`vlib_buffer_attach_clone`, requires head and tail to belong to the same Pool;
it increments every shared tail segment's one-byte atomic reference count.
Shared segments are immutable. Release walks the chain, atomically decrements
each segment, and restores a Pool template only for the segment whose count
reaches zero. Other exclusive chains may span Pools, and every segment returns
through its own `buffer_pool_index`. Invalid chain shape, cross-Pool clone
attachment, reference-count overflow, mutation of a shared segment, and
ownership violations are programmer bugs rather than recoverable `Result`
variants.

The public Buffer free family follows vendored VPP. Rust slices carry the
array length, and ring slices carry `ring_size`:

```rust
impl DataPlaneMain {
    pub fn buffer_free(&mut self, indices: &[u32]);
    pub fn buffer_free_no_next(&mut self, indices: &[u32]);
    pub fn buffer_free_one(&mut self, index: u32);
    pub fn buffer_free_from_ring(&mut self, ring: &[u32], start: usize, count: usize);
    pub fn buffer_free_from_ring_no_next(
        &mut self,
        ring: &[u32],
        start: usize,
        count: usize,
    );
}
```

`buffer_free` and `buffer_free_one` walk the complete chain.
`buffer_free_no_next` releases only the supplied segments. The ring variants
apply the corresponding operation across a wrapping ring range. All five use
the calling `DataPlaneMain` to select the current Worker's per-Pool caches and
delegate to one private Buffer Main implementation. They return slots to
Pools; they do not unmap or deallocate Physmem backing.

### Frame and graph dispatch

Frame layout follows `vlib_frame_t`. Its C-layout header contains separate
`frame_flags` and user `flags`, scalar/vector/aux offsets, `n_vectors`, and the
Frame size-class index. The header is followed by 16-byte-aligned scalar
arguments, 256 vector elements, four speculative vector elements, a debug
magic value, optional auxiliary arguments for 256 elements, and final 64-byte
rounding. Debug allocation poisons the complete allocation before initializing
the header and magic.

`Frame<Scalar = (), Vector = u32, Aux = ()>` itself provides scalar, vector,
and auxiliary argument access. Its three generic parameters carry only
compile-time argument types; their `PhantomData` occupies no bytes and does not
change the C header or trailing allocation. Each accessor computes its address
from the corresponding offset stored in the Frame header, matching
`vlib_frame_{scalar,vector,aux}_args`. `NodeRuntime` does not calculate those
addresses or re-expose the borrows. A zero scalar or auxiliary offset means
that region is absent; the vector offset is always present for an internal
packet Node.

```rust
impl<Scalar, Vector, Aux> Frame<Scalar, Vector, Aux> {
    pub fn scalar_args(&self) -> Option<&Scalar>;
    pub fn scalar_args_mut(&mut self) -> Option<&mut Scalar>;
    pub fn vector_args(&self) -> &[Vector];
    pub fn vector_args_mut(&mut self) -> &mut [Vector];
    pub fn aux_args(&self) -> Option<&[Aux]>;
    pub fn aux_args_mut(&mut self) -> Option<&mut [Aux]>;
}
```

The vector and auxiliary slices contain exactly `n_vectors` initialized
elements. Scalar access returns one registered scalar argument, while auxiliary
access returns one element per vector. The unit scalar and auxiliary defaults
have zero offsets and return `None`.

Every public Frame accessor is safe Rust. Public APIs do not return raw
pointers and do not require a Node or plugin to call an `unsafe fn`. Frame
argument types satisfy the applicable `zerocopy` 0.8 `KnownLayout`,
`FromBytes`, `Immutable`, and `IntoBytes` bounds. A newly allocated Frame has
all argument bytes initialized once before typed access; reused Frame storage
never becomes uninitialized. This permits safe typed writable slices without a
public `MaybeUninit` commit contract. Pointer arithmetic, layout validation,
and typed reference construction remain inside private `hammer-core` or
generated trampoline code with documented safety proofs.

Ordinary packet Frames store contiguous raw `u32` Buffer Indices. They do not
store `Owned<Buffer>`, `Box<Buffer, A>`, a generation, or another per-element
wrapper. A node reads its input vector and writes indices into the writable
suffix of a Next Frame. It does not express ordinary forwarding with
`remove`, `pop`, `drain`, `retain_from`, or equivalent source-container
operations.

The source-level Node function shape is:

```rust
fn(
    &mut DataPlaneMain,
    &mut NodeRuntime,
    &mut Frame<Scalar, Vector, Aux>,
) -> usize
```

For an ordinary packet Node this is
`Frame<(), u32, ()>`. The Node registration macro derives scalar, vector, and
auxiliary byte sizes from those types and emits a private erased trampoline.
The runtime stores and invokes only that trampoline; before constructing the
typed Frame borrow, it validates the installed offsets and allocation size
against the same registration. No public erased Frame cast or unsafe function
pointer crosses into Node or plugin code.

The macro-generated trampoline also contains Node panics inside the plugin
artifact. A Node panic is a programmer bug equivalent to a failed VPP
`ASSERT`: the trampoline calls `std::process::abort()` before unwind can cross
the plugin ABI. It does not attempt to release the input Frame's Buffer
Indices, recycle its Frame, or drain Next/Pending Frames. By the time a Node
panics, some input Indices may already have been copied into Next Frames while
others remain undispatched, so no generic cleanup path can reconstruct the
release obligations without risking double free. The operating system reclaims
the process mappings after termination. Normal refork and shutdown cleanup
therefore operate only on non-panicked graph state.

Getting and putting a Next Frame use the VPP pair directly:

```rust
impl DataPlaneMain {
    pub fn get_next_frame<'a, Vector, Aux>(
        &'a mut self,
        runtime: &mut NodeRuntime,
        next_index: u32,
    ) -> (&'a mut [Vector], Option<&'a mut [Aux]>);
    pub fn put_next_frame(
        &mut self,
        runtime: &mut NodeRuntime,
        next_index: u32,
        n_vectors_left: usize,
    );
}
```

`get_next_frame` validates the destination's registered Vector/Aux layout and
returns its initialized writable vector suffix plus the matching auxiliary
suffix when present. Slice length is VPP's `n_vectors_left` value. The caller
writes a prefix and passes the unused suffix length to `put_next_frame`, which
updates `n_vectors` and schedules a nonempty Frame. Because the Frame allocation
is initialized once and argument types accept every initialized bit pattern, an
incorrect count remains a checked programmer invariant violation rather than a
way for safe caller code to cause an uninitialized typed read. A partial Frame
remains appendable until dispatch claims it. There is no Next Frame guard,
checkout owner, closure API, or public unsafe operation.

`NodeMain.pending_frames` is a Main-Heap-backed `Vec<PendingFrame>` with an
initial capacity of 32, matching VPP's growable pending vector rather than the
current bounded scheduled queue. Putting a nonempty Next Frame appends its
Pending Frame infallibly at the graph API level; there is no queue-full
`Result`. Exhausting the fixed-capacity Main Heap follows Rust allocation-fatal
behavior and terminates the process. Dispatch iterates by Pending Frame index
through the vector's dynamically increasing length because a Node may append
more Pending Frames while it runs. It does not retain a vector element borrow
or pointer across Node dispatch and reloads by index afterward, matching VPP's
explicit handling of vector reallocation.

Next Frame ownership follows VPP's destination enqueue-owner rule. Changing
ownership swaps the previous and new indexed Next Frame states and repairs a
Pending Frame link that referred to the moved state. A Pending Frame contains
the Frame pointer, destination Node Runtime index, and associated Next Frame
index. An empty Frame is never scheduled.

Graph Refork runs at the Worker's barrier check at the start of a main-loop
iteration. The preceding iteration has already dispatched the complete,
dynamically growing Pending Frame vector and reset its length to zero. Refork
does not inspect, drain, free, replace, or shrink `pending_frames`; the same
empty vector and its backing allocation remain available for the next
iteration. This is a scheduler precondition established by main-loop ordering,
not a second Pending Frame cleanup path inside refork.

The Worker then applies the `vlib_worker_thread_node_refork` order to its old
Next Frames:

1. for each entry whose `IS_ALLOCATED` flag is set and whose Frame is present,
   set the Next Frame's Frame field to `None` first, then return only that Frame
   allocation to the free list selected by its `frame_size_index`;
2. discard the old Next Frame vector after all such Frame allocations have
   been detached and recycled;
3. clone the published Next Frame topology, initialize every cloned entry, and
   restore only its destination Node Runtime index and
   `NO_FREE_AFTER_DISPATCH` flag; the Frame field, enqueue-owner flag, pending
   flag, trace flag, and vector accounting start clear;
4. rebuild Node and Node Runtime clones while retaining existing Worker-local
   runtime data, node state, flags, and counters according to ADR-0003.

Refork does not inspect a recycled Frame's vector arguments and never calls the
Buffer free family. The ordinary scheduler path is responsible for dispatching
Pending Frames before the next barrier check; refork defines no separate
recovery, Buffer release, or process-fatal branch for a live packet vector. The
Worker decrements the refork completion count only after the whole clone
replacement returns and no old graph reference remains in Worker state.

Recycling a Frame returns only the Frame allocation to its size class. It does
not release the Buffer Indices present in the vector. Frame memory lifecycle
and packet Buffer release are separate operations.

### Discard and retained ownership

Ordinary packet-processing failures are sent to the terminal Drop Node with
the producing Node's typed error classification. The Drop Node performs trace
and counter work and calls `DataPlaneMain::buffer_free` for the Frame's Buffer
Indices. Ordinary Graph Nodes do not receive feature-specific release helpers.

A real domain object that retains packets beyond a node invocation stores raw
Buffer Indices and owns their release obligation. IP reassembly context is the
canonical example: it accesses Buffer contents only with its owning Worker's
`DataPlaneMain`, transfers the completed chain back to a Frame or Worker
Handoff, and calls the generic Buffer free family for all remaining Indices
before removing the context on failure, timeout, or another owner-controlled
lifecycle transition. Its Rust `Drop` then destroys only ordinary owner-local
storage. Such state does not store borrowed Buffer references.

Worker Handoff transfers raw Buffer Indices to the destination Data Worker
without changing reference counts. Final process shutdown follows VPP and
ADR-0002: the main thread acquires the final Worker Barrier after each Worker
has completed its current loop iteration, including all Pending Frame
dispatch, and never releases it. Main-loop exit functions run while Workers
are permanently parked. Shutdown does not run Worker-local exit callbacks,
drain or free Next Frames, return per-Worker Buffer caches, or traverse retained
domain Buffers. Those values remain unreachable and unchanged until process
termination; the operating system reclaims the process mappings.

There is no explicit backing-memory free API. A scoped `BufferMain` or Buffer
Pool used by startup rollback or isolated tests is destroyed only after its
Buffer users have ended; normal Rust field drop then reaches each owned
`PhysmemMap`, whose `Drop` releases the mapping. The production `BUFFER_MAIN`
has process lifetime and is not dropped during final shutdown.

### Removed alternatives

The design does not use any of the following:

- the current 16-byte generational packet `Index`;
- a separately reserved Buffer Arena address space;
- arena-wide Buffer read/write locks or Buffer access guards;
- positive packet-data `headroom` allocation;
- growable `Vec`-backed packet Frames;
- a public raw-Index Buffer lookup detached from `DataPlaneMain`;
- feature-specific Buffer release helpers beside the generic VPP-shaped free
  family;
- per-Buffer owner wrappers in Frame vector slots;
- `allocator_api2::Box<T, A>` as a Buffer owner;
- `maybe-owned`, `mown`, or a custom owned-or-borrowed enum;
- `retain_from` or other domain-specific Frame extraction APIs.

`allocator_api2::Allocator` is not used to model Buffer Pool ownership.
Physmem mappings own backing regions, while Buffer Pools own slot allocation,
thread caches, templates, reference counts, and recycling. Treating the mapping
as a general allocator would merge these distinct VPP responsibilities;
treating the pool as a `Box` allocator would fail to express shared chain
segments and the explicit Data Worker context used by allocation.

## 变更清单

### 基线与范围

本清单以当前 checkout 为基线。当前公开面由
`hammer_core::data_plane` 统一重导出，Buffer Index、Buffer、Frame 及其
guard/owner 类型会跨 `hammer-core`、`hammer-runtime`、`hammer-service` 和
动态插件边界传播。当前 Node ABI 是
`fn(&DataPlaneMain, NodeRuntimeData, &mut BufferFrame) -> ()`；当前
Buffer Pool 由每个 `DataPlaneMain` 间接持有；当前 handoff slot 存储
`Option<Index>`。

受影响的直接消费者包括 runtime graph fanout、dispatch、trace、handoff，
service Drop Node、interface 与 session，以及 IP、ICMP、TCP、UDP 插件。
这些内存值没有持久化到磁盘，也没有作为 daemon/CLI IPC 消息发送；但它们
属于 Rust/DSO 调用面，因此新旧 daemon、runtime、core 和插件不能混装。

### 新增

#### 类型

| 类型 | 位置或所有者 | 新增内容与不变量 | 兼容性/迁移 | 验证方式 |
| --- | --- | --- | --- | --- |
| `BufferMain` / `BUFFER_MAIN` | `hammer-core::buffer` 中定义的类型与独立进程全局值 | 持有 `buffer_mem_start`、总地址跨度、Pool 表和每个 NUMA 的默认 Pool Index；先于 Data Worker 初始化，所有 `DataPlaneMain` 访问同一全局值；不属于 `GlobalMain` | 新类型和全局权威；替代每个 runtime 自带的 Buffer Arena。它位于 dylib 边界，必须验证 daemon 与插件解析到同一实例 | 多 Worker/动态插件集成测试比较同一 Buffer Index 的地址解析、Pool 表身份和 `BUFFER_MAIN` 地址；初始化回滚测试验证未发布的 Pool 由 Rust Drop 释放 mapping |
| `BufferTemplate` | `hammer-core::buffer`，Buffer Pool 私有 | 精确覆盖 Buffer 第一 cache line；`ref_count = 1`，`buffer_pool_index` 为所属 Pool；分配和最终回收均复制该模板 | 新的私有布局类型；无数据迁移 | `size_of == 64`、`align_of == 64`、字段 offset 与 vendored VPP C probe 一致 |
| `Frame<Scalar = (), Vector = u32, Aux = ()>` | `hammer-core::frame` packet-graph ABI | 泛型参数仅携带参数类型，zero-sized `PhantomData` 不改变 VPP header；尾部区域按注册类型计算并在首次 typed access 前完成一次初始化；vector 固定容纳 256 个元素和 4 个 speculative 元素；Frame allocation 自身不拥有 Buffer 析构行为 | 与现有 `Frame<State>` 同名但语义不同，按“删除旧项 + 新增新项”迁移；普通 packet Node 使用 `Frame<(), u32, ()>`；所有节点和插件重编译 | header offset、总分配长度、泛型实例 size/align 相同、初始化有效性、16/64 字节对齐、256+4 边界测试和 VPP C probe |
| `NodeMain` | `hammer-runtime::node`，每个 Data Worker 独占且不公开导出 | 取代当前由 `NodeRuntime` 承担的 graph-wide 容器职责，拥有 Node Runtimes、Next Frames、Pending Frames 和 Frame size classes | 新的 runtime 内部权威；Node 查询和调度调用方迁移到该 owner，不增加插件可见 API | graph 安装、next owner 交换、pending dispatch、refork 和 final-barrier 生命周期集成测试 |
| `NextFrame` | `hammer-runtime::node`，Worker 私有 | 保存 Frame 指针、目标 Node Runtime、flags、enqueue owner 状态和 overflow 计数；每个目标只允许一个 enqueue owner | 新的内部状态；替代 `Frame<Next>` owner wrapper 和 `appendable_next_frames` 元组 | 多 next fanout、partial put 后继续 append、owner 交换及 pending link 修复测试 |
| `PendingFrame` | `hammer-runtime::node`，Worker 私有 | 保存待 dispatch 的 Frame、目标 Node Runtime index 和关联 Next Frame index；无关联项使用明确 invalid value；存放在 Main Heap 上初始容量 32 的可增长 vector 中，dispatch 按 index 遍历动态长度 | 新的内部状态；替代 `Frame<Pending>` 和当前 scheduled owner；不再存在 queue-full recovery | 空 Frame 不排队、dispatch 中追加、vector reallocation 后 reload、禁用 Node 和 final barrier 前完成 dispatch 测试 |

#### API

| API | 位置或签名 | 输入/输出与行为 | 兼容性/迁移 | 验证方式 |
| --- | --- | --- | --- | --- |
| Buffer layout build configuration and generated constants | `hammer-core/build.rs`; public `BUFFER_PRE_DATA_SIZE`, `BUFFER_TRACE_TRAJECTORY`, `BUFFER_TRACE_TRAJECTORY_SIZE`, and `BUFFER_HEADER_SIZE` | `HAMMER_BUFFER_PRE_DATA_SIZE` defaults to 128 and accepts only cache-line multiples in the signed `current_data` range; `HAMMER_BUFFER_TRACE_TRAJECTORY` defaults to `0` and accepts only `0` or `1`; the build script emits one private trajectory `cfg` and the four public constants from those inputs | New build inputs and public constants; daemon and every plugin must use matching values even when built in separate Cargo invocations | build-script rejection tests for invalid values; trajectory on/off and pre-data variants in the VPP C layout probe; public constant assertions |
| `BufferMain::{new, publish, global}` | `new(usize, usize, &[u32], usize, PageSize) -> DataPlaneResult<Self>`；`publish(self) -> &'static Self`；`global() -> &'static Self` | `new` 构造未发布的 Physmem-backed Pools；`publish` 一次发布到独立 `BUFFER_MAIN`；`global` 取得进程期共享值；重复发布和发布前访问是 startup invariant panic | 新增 safe 启动 API；调用顺序必须先于 graph/Worker 初始化；默认 huge-page fallback 只在未发布值上重试 | 启动顺序、fallback rollback、重复发布/过早访问 expected panic、255/256 Pool 边界、256 GiB 跨度边界及跨 dylib 单例测试 |
| `DataPlaneMain::{buffer, buffer_mut}` | `buffer(&self, u32) -> &Buffer`；`buffer_mut(&mut self, u32) -> &mut Buffer` | 输入 raw Buffer Index，输出直接 borrow，不返回 guard 或 `Result`；mutable borrow 绑定整个 `DataPlaneMain`，必须先结束再操作 Next Frame；无 generation check；invalid/stale/foreign/released/shared-mutable Index 是程序错误 | 新增 safe borrow API并删除 `get_buffer*` guard API；所有调用改名并移除 `?`/guard；节点按 Buffer phase、enqueue phase 缩短借用 | 编译期借用作用域测试、私有 raw-pointer boundary 单元测试、invalid Index expected panic、共享 tail 拒绝 mutable borrow 测试 |
| Buffer allocation family | `DataPlaneMain::{buffer_alloc, buffer_alloc_from_pool, buffer_alloc_on_numa, buffer_alloc_to_ring, buffer_alloc_to_ring_from_pool}`，签名见 Decision | 接收调用方已拥有的 initialized `u32` slice、可选 Pool/NUMA 与 ring range，返回被新 Index 覆盖的前缀长度；Pool 压力以 short count 表达 | 新增完整 VPP-shaped batch allocation family；替代返回单个裸 Index 的普通公开路径，不产生成功但无人承担 release obligation 的 Index | 0、部分、完整批量分配；default/NUMA/Pool selection；wrapping ring；跨 cache refill；未返回 suffix 保持不变；Pool 压力不返回逐 Buffer `Err` |
| `Frame::{scalar_args, scalar_args_mut, vector_args, vector_args_mut, aux_args, aux_args_mut}` | `scalar_args(&self) -> Option<&Scalar>`；`scalar_args_mut(&mut self) -> Option<&mut Scalar>`；`vector_args(&self) -> &[Vector]`；`vector_args_mut(&mut self) -> &mut [Vector]`；`aux_args(&self) -> Option<&[Aux]>`；`aux_args_mut(&mut self) -> Option<&mut [Aux]>` | `Frame` 自己从 header offset 返回带正确生命周期的 typed borrow；vector/aux slices 恰含 `n_vectors` 个已初始化元素；泛型参数与 `zerocopy` traits 约束 layout/bit validity；所有公开方法均为 safe Rust且不返回裸指针 | 新增 ABI 访问 API；Node 宏将参数类型与注册尺寸绑定并生成私有 erased trampoline；调用者不编写 unsafe | 不同参数类型/大小的 graph 安装和真实 node invocation；错误 trampoline/layout expected panic；compile test 证明 public API 无 unsafe；zero-size 区域和 initialized-byte 测试 |
| Buffer free family | `DataPlaneMain::{buffer_free(&mut self, &[u32]), buffer_free_no_next(&mut self, &[u32]), buffer_free_one(&mut self, u32), buffer_free_from_ring(&mut self, &[u32], usize, usize), buffer_free_from_ring_no_next(&mut self, &[u32], usize, usize)}` | 对应 vendored VPP 的五个公开 free helpers；`DataPlaneMain` 选择当前 Worker cache；chain/no-next/ring 语义分别保持一致；共同调用一个私有 Buffer Main implementation；invalid Index/refcount/chain 是程序错误且不返回 `Result` | 新增完整 generic free family；不新增 feature-specific release API。free 只回收 Pool slot，不释放 Physmem mapping | empty/single/batch、chain/no-next、wrapping ring、跨 Pool chain、shared tail、trace finalization 与 refcount-to-zero 测试 |
| `Buffer::{reset, space_left_at_end, put_uninit, push_uninit, make_headroom, pull}` | `reset(&mut self)`；`space_left_at_end(&self) -> usize`；`put_uninit(&mut self, u16) -> &mut [u8]`；`push_uninit(&mut self, u8) -> &mut [u8]`；`make_headroom(&mut self, u8) -> &mut [u8]`；`pull(&mut self, u8) -> Option<&[u8]>` | 对应 vendored VPP 的单段 data-window helpers；capacity 通过 `buffer_pool_index` 查询 `BUFFER_MAIN`；返回 safe slice；`put_uninit`/`push_uninit` 在返回前立即更新长度；越界为 invariant panic | 新增 safe public API；替代 writable-tail/commit 与 prepend 两阶段或复制式接口；调用方必须在返回 slice 的 borrow 结束后再次操作 Buffer | reset 正负 offset、tail 恰满/越界、push 到 pre-data 边界、make-headroom 长度不变、pull 成功/不足不变测试 |
| Buffer chain data append family | `DataPlaneMain::{buffer_add_data(&mut self, &mut u32, &[u8]) -> usize, buffer_chain_append_data(&mut self, u32, u32, &[u8]) -> usize, buffer_chain_append_data_with_alloc(&mut self, u32, &mut u32, &[u8]) -> usize}` | 对应 vendored VPP 的 add/append/append-with-alloc；返回实际追加字节数；Pool pressure 保留已追加前缀；新 head/last 在分配并链接后立即写回，确保 release obligation 可达；所有公开面为 safe Rust | 新增通用 chain API；替代 feature-specific chain copy/rebuild；调用方必须处理 short count，并在放弃 partial chain 时调用 generic free family | existing/invalid head、末段 full/partial、跨多段、首段 Pool 定向分配、default Pool 扩链、allocation shortfall partial retention 与 exact-once free 测试 |

### 修改

#### 类型

| 类型 | 当前/目标位置 | 修改内容与不变量 | 兼容性/迁移 | 验证方式 |
| --- | --- | --- | --- | --- |
| `Buffer` | `hammer-core::buffer` | 从两个 64-byte header 变为完整 VPP 顺序：template overlay、second half、可选 64-byte trajectory、64-byte `headroom` align mark、内联 `pre_data[BUFFER_PRE_DATA_SIZE]`，随后是同一 Pool allocation 的 `data[]`；`current_data: i16` 相对 `data[0]`，允许负值 | 二进制布局破坏；所有访问 Buffer 的 crate/DSO 必须一起重编译；无持久化迁移 | 两种 trajectory 配置下的 size/align/offset C probe；`pre_data.end == data.start`；负 `current_data` prepend 测试 |
| `BufferFlags` | `hammer-core::buffer` | 仅保存 core/VPP-style 公开 flags；删除 `SLOT_CLEAN` 和高位私有 data-capacity 编码；second half/opaque 有效性由公开 flags 和所属操作保证 | 行为及 bit contract 破坏；插件不得依赖旧私有位 | bit round-trip、template restore、未初始化 second half 只在有效 flag 下读取的测试 |
| `PhysmemMap` | `hammer-infra::physmem` | 继续拥有 mapping/fd/page/NUMA 生命周期，但其 Buffer 用途改为由进程 Buffer Main 注册、由 Buffer Pool 切槽；不承担 slot allocate/recycle；其 `Drop` 是 backing mapping 真正释放点 | 现有 OS mapping API原则上可复用；若无需字段/API变化则实现阶段不得修改 | mapping ownership、page size、NUMA 与 drop/unmap 测试；Buffer Pool 仅持有有效 mapping 关系 |
| `BufferPool` | `hammer-core::buffer`，由 `BufferMain` 持有 | 从 `BufferPoolArena + Rc<RefCell<BufferThreadCache>>` façade 改为 VPP Pool：mapping 范围、Pool index、data/alloc size、central free indices、每 Worker cache、lock 和 template；每段按自己的 Pool 回收 | 私有结构完全重排；删除 arena lock 访问模式 | page-crossing slot skip、Index zero skip、central/cache batch、cross-Pool chain release 测试 |
| `BufferThreadCache` | `hammer-core::buffer`，每 Pool 每 Worker | 直接保存 raw `u32` indices 和数量，由 Worker 生命周期对应的 cache slot 使用；不再 `Rc<RefCell<_>>` clone | 私有结构破坏；Worker 创建时建立 cache；生产 final shutdown 不归还 parked Worker cache，scoped owner drop 正常销毁 storage | Worker 并行 alloc/recycle、cache high-water、final barrier 后 cache 不再访问、scoped Pool drop 测试 |
| `FramePool` | runtime `NodeMain` | 从 generational `FrameSlot { Option<BufferFrame> }` Pool 改为按 frame allocation size 分类的 raw Frame 重用；recycle 只回收 Frame memory | 私有结构及错误路径破坏；不再以 packet `Index` 标识 Frame slot | 不同 Node frame size 的重复分配、debug poison/magic、回收不触碰 Buffer 测试 |
| `DataPlaneBufferConfig` / `WorkerBuffer` | `hammer-runtime::data_plane::config` / startup config | 从“每个 DataPlaneMain 构造 arenas”改为“进程启动构造 Physmem-backed Pools，再给 Worker 建立对应 cache”；`slot_bytes` 表示 Pool data size；`frame_pool_size` 表示每个 lazily-created Frame size-class free list 的初始 reserve capacity | TOML 字段和 aliases 保留，无配置文本迁移；初始化时序和 `frame_pool_size` 行为改变 | config parse、Frame size-class initial reserve、NUMA Pool 建立、默认 hugepage fallback 和所有 Worker 共享同一 Buffer Main 测试 |
| `DataPlaneMain` | `hammer-runtime` | 改为不可通过 `Clone` 扩散 packet-path owner；Node ABI 以 `&mut DataPlaneMain` 进入；持有 Worker-local execution/cache state并访问同一进程 Buffer Main | Rust API 与借用模型破坏；所有 Node、trace、handoff、session 及 plugin 调用点迁移 | compile-time `!Clone` 约束、每 Worker 独占、handoff 后仅目标 Worker mutable access 测试 |
| `GlobalMain` | `hammer-runtime` | 不增加 `BufferMain` 字段或所有权；启动编排只要求独立 `BUFFER_MAIN` 在 Worker 前完成初始化；final shutdown 永久停车 Workers，且不遍历或销毁进程期 `BUFFER_MAIN` | 生命周期顺序改变；daemon/runtime 一起重编译，无持久化迁移 | 启动顺序、final barrier 永不释放、worker panic 和 partial startup rollback 测试 |
| `NodeRuntime` | `hammer-runtime::node` | 从 graph-wide `Rc<RefCell<NodeRuntimeInner>>` façade 改为单个 Node 的 Worker-local runtime，作为 node function 第二个可变参数；持有 node identity、function/error tables、next-frame range/cache、state/flags、dispatch cadence/reason、counters 和 node-local runtime bytes；Rust 私有字段顺序不属于 ADR contract | 语义及 ABI 破坏；原 graph-wide 查询/注册职责迁移到 `NodeMain` | node-local runtime mutation、next-frame index/cache、dispatch state、统计和 graph refork state 保留测试 |
| `NodeRegistration` / `NodeEntry` | `hammer-core::graph` / `hammer-runtime::node` | registration 增加由 `Frame<Scalar, Vector, Aux>` 推导的 scalar/vector/aux element byte sizes；安装时据此计算 Frame offsets、总大小和 size-class index；siblings 共享 next arcs，但保留各自 frame layout | 公开构造器和 proc-macro 展开变化；所有注册项重编译 | 普通 packet Node 的 0/4/0 布局、custom arguments、sibling 不同 frame layout 测试 |
| `NodeFunctionRegistration` | `hammer-runtime::node` | SIMD candidate 保存宏生成的私有 erased trampoline；trampoline 校验 Frame layout 后调用 typed safe Node function；在插件 artifact 内捕获 Node panic 并立即 abort，禁止 unwind 跨 plugin ABI；CPU variant 选择规则不变 | 公开 `#[doc(hidden)]` ABI 破坏；宏生成和插件注册整体重编译；unsafe 仅在框架私有边界；Node panic 从 Worker unwind 改为进程 fatal | scalar/SIMD variant 注册、layout mismatch expected panic、重复 variant、真实 dylib load 调用及子进程 Node-panic abort 测试 |
| `PluginMetadata` | `hammer-runtime::plugin` | 增加 fixed-width `buffer_pre_data_size`、`buffer_trajectory_size`、`buffer_size` 和 `buffer_alignment`，值来自该 artifact 编译时的 `hammer-core` constants | Cross-DSO ABI 破坏；daemon 和全部 plugins 必须一起重编译，package semver 相同也不能跳过 layout comparison | matching layout load succeeds；逐字段 mismatch 在读取 registration image 前拒绝；跨独立 Cargo invocation 集成测试 |
| `HandoffSlot` / `HandoffFrame` | `hammer-runtime::handoff` | slot element 从 `[Option<Index>; 32]` 改为 raw `u32` prefix + length；handoff 只转移 release obligation，不改 refcount | 进程内队列布局破坏；不做新旧版本互通 | full/partial slot、queue full failure-atomicity、跨 Worker ownership 与 wakeup 测试 |
| `DropNode` | `hammer-service::data_plane` | 从仅 trace 后依赖 incoming `Frame` owner 隐式释放，改为显式批量结束每个 Buffer chain 生命周期；Frame memory 随后独立 recycle | 行为变化；所有 drop/punt/error arcs 必须确认最终 disposition | node error counter、trace、chain release、shared tail 和 Frame reuse 集成测试 |
| `FragmentContext` / `ReassemblyFragment` | IP plugin reassembly | 继续保存 raw Buffer Indices；完成时转移，失败/超时/显式 owner removal 时在持有 `&mut DataPlaneMain` 的 owner lifecycle 中批量 free 剩余 Indices，再由 Rust `Drop` 销毁空 context 的普通 storage；production final shutdown 不遍历 parked Worker state | 插件内部生命周期变化；删除通过伪 Next Frame 聚合后隐式回收的做法，不增加 owner wrapper 或 reassembly-specific release API | complete、duplicate、overlap、timeout、context removal 的精确一次 release；final barrier 不触碰 retained state |
| `BufferInvariant` / `DataPlaneError` | `hammer-core::error` | 删除 generation/arena/checked-out-owner、`FramePoolExhausted` 和 scheduled-queue 类 variants；增加 `BufferAllocationSizeOverflow { data_size }`、`BufferAllocationExceedsPage { allocation_size, page_size }`、`BufferPoolCountExceeded { requested, maximum }`、`BufferMemorySpanExceeded { bytes, maximum }`、`BufferPoolsUnavailable`、`BufferPoolMapping { numa_node, source }`；已建立 ownership 后的 invalid raw Index、共享写、chain shape 和 refcount overflow 是程序错误 | error enum 是公开破坏；startup caller 只处理配置、mapping 和 Pool 建立失败；packet path 删除旧 `Result` 分支 | concrete variant/字段匹配、`BufferPoolMapping` source chain、unpublished construction failure atomicity 和 invariant expected-panic 测试；不做 display-string-only 测试 |
| `PluginError` | `hammer-runtime::plugin` | 增加 `BufferLayoutMismatch`，携带 host/plugin 的 pre-data size、trajectory size、Buffer size 和 alignment；在任何 metadata/registration publication 前返回 | 新 recoverable load rejection；调用方保留现有 plugin-load recovery path，无字符串匹配 | 匹配 concrete variant and fields；确认 source transaction 未发布 library metadata 或 registrations；更正 artifact 后可重试 |

#### API

| API | 当前/目标签名或标识 | 修改内容 | 兼容性/迁移 | 验证方式 |
| --- | --- | --- | --- | --- |
| `Buffer::{current, current_mut}` | `hammer-core::buffer::Buffer` | 数据基址改为内联 `data[0]`，再应用 signed `current_data`；只返回当前窗口的 safe slice，不再从 128-byte header 后另算抽象 writable region | 名称保留，布局语义与私有 unsafe proof 重写 | 最小/最大负 offset、data start、current length bounds 测试 |
| `Buffer::advance` | 从 `advance(&mut self, isize) -> DataPlaneResult<()>` 到 `advance(&mut self, isize)` | 精确采用 VPP signed displacement：正值消费头部，负值向 pre-data 回退；headroom 为 `BUFFER_PRE_DATA_SIZE + current_data`；越界为 programmer invariant panic | 返回类型破坏；调用方删除 `?`/错误分支，并重新验证正负 displacement 的 owner invariant | 前进到空、回退到 pre-data 边界、超界 expected panic、chain 最小段不变量测试 |
| `buffer_chain_init` / `buffer_chain_buffer` / `buffer_attach_clone` | `DataPlaneMain` safe methods，签名见 Decision | raw `u32` exclusive chain link 转移 release obligation；clone attachment 要求同 Pool 并对所有 shared tail segments 原子加一；其他 exclusive chains 可跨 Pool | receiver/name/return contract 变化；现有调用全部迁移；不新增 feature-specific ownership API | chain totals、cross-Pool exclusive chain、cross-Pool clone rejection、refcount overflow、shared mutation rejection 测试 |
| `get_next_frame` / `put_next_frame` | `DataPlaneMain::get_next_frame<'a, Vector, Aux>(&'a mut self, &mut NodeRuntime, u32) -> (&'a mut [Vector], Option<&'a mut [Aux]>)`；`put_next_frame(&mut self, &mut NodeRuntime, u32, usize)` | get 校验注册 layout 并借出已初始化 writable suffix；slice length 和 put 的参数都使用 VPP `n_vectors_left` 语义；put 更新 `n_vectors` 并按 owner/pending 规则调度；无 guard | 完全破坏签名；fanout、driver、handoff、tests/benches 全量迁移 | partial/full frame、错误 remaining count、layout mismatch、speculative slots、owner swap、pending repair 和 pending vector 动态增长测试 |
| `Node::process` / `NodeProcessFn` / internal `NodeFunction` | 从 `fn(&DataPlaneMain, NodeRuntimeData, &mut BufferFrame) -> ()` 到 `fn(&mut DataPlaneMain, &mut NodeRuntime, &mut Frame) -> usize` | 返回处理 vector 数；节点可通过其 runtime 使用 next-frame state；删除复制式 `NodeRuntimeData` 参数 | 公共 Rust/DSO ABI 破坏；所有 Node impl、descriptor、macro expansion 与 function variants 重编译 | 所有 Node 真实调用、return count 统计、panic boundary 和 dylib integration |
| `NodeDescriptor::{new, process, runtime_data}` 及 graph registration APIs | `hammer-runtime::node` | descriptor 不再携带复制式 `NodeRuntimeData`；Node Main 安装 per-node runtime 和 Frame layout | 构造器/访问器破坏；测试节点和手工注册点迁移 | graph install/refork、node-local state 保存、descriptor registration 测试 |
| `process_frame!` / `DataPlaneMain::enqueue_to_next` | runtime graph fanout | 输入 Frame 只读遍历，按 next 分组复制 raw indices 到 Next Frames；不再 `discard_prefix`、`retain_*` 或 drain 输入 | 宏展开和 fanout 行为变化；所有 node call sites 重编译 | 单 next、多 next、spill、speculative enqueue 和输入 `n_vectors` 不变测试 |
| `DataPlaneMain::{new, try_new, for_worker}` | runtime startup/worker creation | 不再为每个 clone/runtime 创建 Buffer arenas；连接一次发布的 Buffer Main 与该 Worker 的 cache/Node Main | 生命周期破坏；启动、worker spawn、graph refork 调整顺序 | main + N workers 的单一 Buffer Main、worker start/exit 顺序测试 |
| `DataPlaneMain::refork_worker_graph` / `NodeMain::refork` | Worker barrier release 后的 graph refork | main-loop ordering 保证 Pending vector 已 drain 并清零；refork 不访问或替换它；对每个 `IS_ALLOCATED` 且 Frame 存在的旧 Next Frame 先断开 Frame 再按 `frame_size_index` 回收 Frame allocation；替换旧 Next Frame vector；新项仅保留 destination runtime index 与 `NO_FREE_AFTER_DISPATCH`；不读取 vector arguments、不调用 Buffer free；整个替换返回后才递减 completion count | 细化 ADR-0003 的 Frame/Buffer 生命周期；旧 `NodeRuntime::refork` 迁移到新的 `NodeMain` owner，继续保持无 `Result`；不增加 invalid-live-frame 错误或清理 API | 按 vendored `vlib_worker_thread_node_refork` 和 main-loop ordering 补充 Pending dispatch boundary、old-frame detach/recycle、Pending storage identity、new-next initialization、Buffer non-release、runtime-state preservation 与 completion ordering 测试 |
| `handoff_frame` / `handoff_index` | `hammer-runtime::DataPlaneMain` | raw `u32` handoff；成功后 source 停止访问，失败保持 source obligation；不通过删除 input Frame 元素表达转移 | 行为和借用约束变化；所有 handoff node 迁移 | queue full rollback、成功转移、目标 dispatch、final barrier 后不再访问队列测试 |
| trace/error Buffer APIs | runtime trace 与 service `set_*_node_error` | 改用直接 Buffer borrow；trace validity 由 `TRACED` flag 控制，Drop Node 在最终回收前完成 trace | guard/result 传播变化；packet invariant 不再伪装为 recoverable lookup error | traced drop、untraced drop、error index、shared tail trace 测试 |
| startup TOML `worker.buffer.*` | `WorkerBuffer` serde schema | 保留 `slot_bytes`/alias `data_size`、`slots_per_numa`/alias `buffers_per_numa`、`frame_pool_size`、`page_size`；语义改为进程 Pool 和 Frame size-class 初始容量 | 无配置文本迁移；旧名称和 aliases 均继续解析 | existing config fixtures parse、unknown field rejection、round-trip 测试 |

### 删除

#### 类型

| 类型 | 当前所在位置 | 删除原因 | 兼容性/迁移 | 验证方式 |
| --- | --- | --- | --- | --- |
| `Index { pool_id, slot, generation }` | `hammer-core::buffer::index`，公开 | Frame ABI 需要 raw `u32`；Pool 与 generation 不再编码在 identity 中 | 无 alias/兼容层；所有字段访问和 16-byte layout 使用改为 Buffer header/Buffer Main 查询 | compile-time `u32` frame element checks；旧构造与 getter 不再编译 |
| `BufferPoolArena` / `BufferPoolInner` | `hammer-core::buffer` | arena-wide `Arc<RwSpinlock<_>>` 与单 Pool guarded lookup 不符合进程 Buffer Main + per-worker cache | 删除公开构造和 clone；调用者改走启动期 Buffer Main/Pool 配置 | 公共 API compile-fail check；hot path profile 不出现 arena lock |
| `BufferRef<'a>` / `BufferRefMut<'a>` | `hammer-core::buffer`，公开 | 仅为观察/修改 Buffer 包装 lock guard；目标模型使用 Rust `&Buffer`/`&mut Buffer` | 无兼容 wrapper | public export check；节点调用编译时只能取得直接 borrow |
| `DataPlaneBuffers` | `hammer-core::buffer`，公开 | 它是每 runtime 的 arena/frame wrapper，并非 VPP 的进程 Buffer Main 或 worker Node Main | 删除 `DataPlaneMain::buffers()` 返回面；调用分别迁移到 Buffer/Node owner | workspace API migration 编译；无该公开 export |
| `BufferFrame` | `hammer-core::buffer::frame`，公开 | `Vec<Index>`、可变逻辑 capacity 和 retain/drain 语义与固定 VPP Frame 不同 | 无 type alias；全部替换为新的 ABI `Frame` | 所有节点/插件重编译；allocation/profile 检查无 packet-path Vec growth |
| `Frame<Next>` / `Frame<Pending>`、`Next`、`Pending` | `hammer-core::buffer::checked_out`，公开 | 将 Frame memory 与 Buffer release 混为 owner wrapper，并靠 wrapper Drop 隐式 free packet | 无兼容层；runtime 改用 `NextFrame`/`PendingFrame` 内部状态和独立 Frame recycle | 旧类型不再导出；Frame recycle 不改变 Buffer refcount |
| `FrameSlot` / `FramePoolInner` 的 generation/allocated 模型 | `hammer-core::buffer`，私有 | 新 Frame 按 layout size class 重用，不使用 packet-style Index/generation | 私有替换 | size-class reuse 和 debug magic 测试 |
| `BufferSlot { generation, allocated }` | `hammer-core::buffer`，私有 | raw Buffer Index 不做 generation validation；slot 状态由 Pool free sets、refcount 和可选 debug validation表达 | 私有替换；旧 stale-index 行为删除 | allocation/recycle stress；debug double-release invariant |
| `BufferHeaderCacheline0` / `BufferHeaderCacheline1` | `hammer-core::buffer::header`，私有 | 被精确 template overlay + second-half + trajectory + pre-data 的一个 `Buffer` layout 取代 | 私有布局破坏 | VPP layout probe |
| `BufferChain<'_>` | `hammer-core::buffer::chain`，私有 | iterator 返回 lock guards；新模型由 `DataPlaneMain` methods 在一个受限 borrow 内沿 raw `next_buffer` 遍历，不缓存 owner pointer/offset wrapper | 删除且不增加替代 iterator type 或公开 API | chain traversal、cycle/invalid shape invariant 测试 |
| `NodeRuntimeData` | `hammer-runtime::node`，公开 | 四个复制 word 不能表达 VPP per-node mutable runtime；状态并入 `NodeRuntime` | 无 alias；所有 node constructors 和 tests 迁移 | old API compile-fail；node-local mutable state test |
| `NodeRuntimeInner` / `NodeRuntimeSlot` 的当前 graph façade 结构 | `hammer-runtime::node`，私有 | graph-wide ownership迁移到 `NodeMain`，单-node dispatch state 由新的 `NodeRuntime` 表达 | 私有结构替换 | graph install/refork/dispatch tests |
| `HandoffSlotGuard` | `hammer-runtime::data_plane`，私有 | 当前 guard 的 Drop 依赖手工 `drop_index_owned`; 新 handoff failure 必须直接保留或转移明确 obligation | 私有替换；不能以新 owner wrapper 绕回 Frame ABI | queue error exact-once release tests |

#### API

| API | 当前标识 | 删除内容 | 兼容性/迁移 | 验证方式 |
| --- | --- | --- | --- | --- |
| Index getters | `Index::{pool_id, slot, generation}` | 连同 16-byte `Index` 删除 | 调用方直接传递 `u32`；Pool provenance 从 Buffer header读取 | old API compile-fail |
| Buffer Arena constructors/introspection | `BufferPoolArena::{with_capacity, with_capacity_on_numa, pool_id, numa_node}` | 删除每调用方创建/clone arena 的公开能力 | 启动配置统一创建 Buffer Pools | public API compile-fail；单一 Buffer Main integration |
| `DataPlaneBuffers` API | `from_arenas`, `with_active_buffer_arena`, `alloc_index*`, `get_buffer*`, `chain`, `get_next_frame`, `drop_index_owned_with_trace` 等全部 inherent public methods | 删除 wrapper 的完整公开方法集，职责分别归 Buffer Main、DataPlaneMain 借用、Node Main Frame lifecycle | 直接消费者逐项迁移；不保留 forwarding methods | workspace build 及 public rustdoc/API snapshot |
| `BufferFrame` API | `with_capacity`, `push_index`, `push_indices`, `indices`, `discard_prefix`, `retain_indices*`, `buffer_node_inline`, `rewrite_indices_batched` 及 pool-reset/drain internals | 删除 growable container 与 input mutation 模型 | 使用 Frame vector borrow + explicit Next Frame enqueue | old API compile-fail；fanout behavior tests |
| checked-out Frame API | `Frame<Next>::next`, `into_pending`, `Frame<Pending>::return_with_trace_release` 及相关 `Deref`/`Drop` | 删除 wrapper ownership 和 Frame-drop-free-buffer 行为 | runtime 显式 get/put/dispatch/recycle | Frame recycle/refcount independence tests |
| direct single-Index ordinary allocation | `DataPlaneMain::{alloc_index, alloc_index_with_bytes}` 及对应 `DataPlaneBuffers` 方法 | 普通生产、测试、driver 和 benchmark 路径统一改用 caller-owned Frame/ring/slice 上的 batch allocation family；不保留返回裸单个 Index 的入口 | 破坏现有 driver/tests/benches；不添加 `Owned<Buffer>` 兼容层或测试专用例外 | allocation obligation tests；old API compile-fail |
| guarded Buffer lookup | `DataPlaneMain::{get_buffer, get_buffer_mut}` 及对应 `DataPlaneBuffers` 方法 | 删除 `Result<BufferRef*>` lock-guard surface | 迁移到 `DataPlaneMain::{buffer, buffer_mut}`；invalid Index 为 invariant | workspace compile；invalid-index expected panic |
| 分散的 release/drop helpers | `DataPlaneBuffers::drop_index_owned_with_trace`, `DataPlaneMain::drop_index_owned`, `drop_pending_frame_owned`, `drop_handoff_slot_owned` | 删除按容器或场景分裂的 helper；统一为 `DataPlaneMain` 上的 VPP-shaped generic free family | 现有调用迁移到 chain/no-next/single/ring 中语义匹配的一项；不保留 feature-specific cleanup API | public API snapshot；Drop Node/retaining-owner exact-once tests |
| positive-headroom reset/capacity internals | `DEFAULT_PACKET_HEADROOM`, `Buffer::reset_empty(..., headroom)`, `reset_empty_fast`, `set_data_capacity`, `data_capacity`, `BufferFlags::{with_private_data_capacity, private_data_capacity}` | 删除 data 内正 offset headroom 和 flags capacity 编码 | caller 使用 `current_data = 0` 初始状态、inline pre-data 和 Pool data size | layout/headroom/capacity tests |
| ambiguous current-data getter | `Buffer::current_data() -> usize` | 删除将负 offset 截断为零的 API；signed field observation 使用 `current_data_offset() -> i16`，packet bytes 使用 `current()` | 调用方按 signed offset 或 `current()` 访问 | negative-offset tests；old API compile-fail |
| transactional writable-tail API | `Buffer::{writable_tail_mut, commit_writable_tail}` | VPP 没有 writable-tail/commit 两阶段语义；删除两者，不保留同义改名 | TCP reset 等调用迁移到 `put_uninit` 或 chain add-data family | old API compile-fail；替代操作的 capacity、length 和 chain 测试 |
| copied prepend API | `Buffer::{prepend, prepend_mut}` | 删除与 VPP 不一致的复制式/`usize` prepend surface；由 `push_uninit(u8)` 返回新前缀 slice 并立即更新窗口 | 调用方改为 `push_uninit` 后直接写入返回 slice | old API compile-fail；header prepend 行为测试 |
| public Buffer pointer getters | `Buffer::{current_ptr, current_mut_ptr}` | 删除公开 raw pointer；地址计算和 slice 构造只留在 `hammer-core` 私有实现 | 外部调用改用 `current`、`current_mut` 或本 ADR 定义的 data-window safe slice API | old API compile-fail；public API snapshot；Miri/边界测试覆盖私有 unsafe proof |
| copied node runtime API | `NodeRuntimeData::{empty, from_words, from_usize, word, usize_word}`、`Node::node_runtime_data`、`NodeDescriptor::runtime_data` | 删除复制式四 word state | node state 放在单-node `NodeRuntime` | all Node registrations compile；state mutation tests |
| old Node function ABI | 所有 `fn(&DataPlaneMain, NodeRuntimeData, &mut BufferFrame) -> ()` function pointers 与宏生成项 | 删除旧 ABI 和适配 shim | daemon/core/runtime/service/plugins 必须原子升级并全量重编译 | real dylib load and invoke test；不只做源码匹配 |
| bounded scheduled Frame queue | `ScheduledFrameQueue`、`scheduled_frame_queue_capacity`、`DataPlaneError::ScheduledFrameQueueExhausted` 及 queue-full `Result` 分支 | 删除固定容量 ring 和可恢复 queue-full 模型；由 `NodeMain.pending_frames: Vec<PendingFrame>` 直接表达 VPP pending vector | `put_next_frame` 和内部 schedule path 不再返回 queue exhaustion；调用方删除 `?`、`is_err` 及 fallback free 分支 | 超过旧容量持续 dispatch、dispatch 中 vector 增长、Main Heap allocation-fatal 子进程测试；old error variant compile-fail |
| Worker-local exit callback API | `DataPlaneMain::{register_worker_exit_function, take_worker_exit_functions}`、`run_worker_exit_functions` 及 callback storage | 删除与 VPP final-barrier shutdown 冲突的 restartable Worker cleanup path；生产 shutdown 永久停车 Worker 并直接进入进程退出 | IP/transport 等现有 callback 注册删除；需要在正常运行期结束的 domain lifecycle 由其 owner 在 final shutdown 前显式完成，不能依赖 parked Worker callback | public API compile-fail；subprocess 验证 final barrier 后 callback 不执行且进程正常退出 |

## 兼容性、迁移与发布

这是一次有意的破坏性替换，不提供旧 `Index`、Buffer guard、
`BufferFrame` 或旧 Node ABI 的 deprecated alias/shim。内存中没有需要迁移
的持久化 packet/frame 数据；进程重启会自然丢弃旧布局。startup TOML
字段和 aliases 保留，因此没有配置文件文本迁移。

发布单位必须包含 daemon、`hammer-core`、`hammer-runtime`、
`hammer-service` 与所有 packet graph 插件。由于 Node function pointer、
Frame layout 和 Buffer Index width 都跨 dylib，不能滚动混装旧插件。发布前
需要真实 `dlopen`/root-module/graph invocation 集成测试，验证所有 artifact
由同一 ABI 与 compile-time Buffer 配置构建。回滚同样需要恢复整套 artifact
并重启进程；没有磁盘数据回滚步骤。

`hammer-core` 新增对 `zerocopy` 0.8 的直接依赖并公开使用其 trait bounds；
这使相关 traits 成为 source-level API contract。daemon 与所有插件必须由同一
workspace lockfile 重编译，不依赖当前仅由其他 crate 间接带入的版本。

实现迁移顺序按依赖方向进行：先完成 `hammer-infra` Physmem 所需的通用能力
和 `hammer-core` ABI，再完成 runtime Node/Frame lifecycle，随后迁移 service
和各插件，最后移除旧 API。中间提交不得作为可运行的混合版本发布。

## 验收检查

- `Buffer`、`BufferTemplate` 和 `Frame` 的每个字段 offset、size、alignment
  在 trajectory 开/关与最终 `BUFFER_PRE_DATA_SIZE` 配置下匹配 vendored VPP
  C probe；`pre_data` 必须紧邻 trailing `data[]`。
- Buffer Index 地址换算覆盖零 invalid、首尾合法 index、mapping gaps、跨 Pool
  和 256 GiB 上限；Pool 数覆盖 255 成功与第 256 个失败。
- Pool 测试覆盖自然 stride 对齐、跳过跨页 slot、per-worker cache refill/
  drain、short allocation、template restore 和 Pool pressure failure atomicity。
- chain/clone 测试覆盖跨 Pool chain、tail refcount 增减、shared tail 只读、
  overflow/double release/invalid shape 的程序错误边界。
- Frame/Node 测试覆盖 scalar/vector/aux offset、256+4 slots、debug poison/magic、
  get/put partial append、owner swap、Pending Frame link repair、dispatch count 和
  Frame recycle 不释放 Buffer。
- Graph Refork 按下表补充行为测试。测试名称使用 Hammer 领域命名，测试断言逐项
  对应 vendored VPP 的 main-loop、`vlib_worker_thread_node_refork`、
  `vlib_next_frame_init` 和 `vlib_frame_free` 行为：

| 测试 | 层级与设置 | 必须断言的行为 | VPP 依据 |
| --- | --- | --- | --- |
| `worker_reforks_after_pending_dispatch` | `hammer-runtime` 多线程集成测试；让内部 Node 产生后续 Pending Frame，同时由主线程请求 graph update | Worker 完成本轮动态增长的 Pending dispatch 并将长度清零，下一轮顶部 barrier check 后才 refork；每个已调度 Frame 只 dispatch 一次 | `main.c::vlib_main_or_worker_loop` 在循环顶部检查 barrier，并在循环尾按动态长度 dispatch 后清零 `pending_frames` |
| `refork_recycles_allocated_next_frames` | `NodeMain` 单元测试；多个旧 Next Frames 覆盖无 Frame、不同 Frame size class，以及 `IS_ALLOCATED` 且 Frame 存在的正常可复用状态 | 仅满足 `IS_ALLOCATED && frame.is_some()` 的 Frame 被先从 Next Frame 断开，再回到各自 `frame_size_index` free list；旧 Next Frame vector 随后被替换 | `threads.c::vlib_worker_thread_node_refork` 的旧 Next Frame 循环及 `main.c::vlib_frame_free` |
| `refork_preserves_pending_frame_storage` | `NodeMain` 单元测试；在已完成 dispatch 后记录空 Pending vector 的 allocation identity 与 capacity | refork 前后 length 都是零，allocation identity 与 capacity 不变；refork 不遍历、重建或释放 Pending vector | `vlib_worker_thread_node_refork` 不访问 `pending_frames`；worker 初始 clone 单独创建该 vector |
| `refork_initializes_published_next_frames` | `NodeMain` 单元测试；published graph 含多个 destination runtime indices，并分别设置/不设置 `NO_FREE_AFTER_DISPATCH` | 新 Next Frames 的 destination runtime index 和该 policy bit 与 published graph 一致；Frame、owner、pending、trace、其他 flags 及 vector accounting 全部清零 | `threads.c::vlib_worker_thread_node_refork` 复制后调用 `vlib_next_frame_init`，只恢复 runtime index 和 policy bit |
| `refork_does_not_release_packet_buffers` | Buffer/Frame 集成测试；记录 Buffer refcount、Pool free count，并在旧 Frame 未使用的 vector slots 放置可识别 Index | refork 只回收 Frame allocation，不读取 vector slots；Buffer refcount 与 Pool free count 均不变化 | `main.c::vlib_frame_free` 只清 Frame flags 并把 Frame 放回 size-class free list；Buffer release 是独立 API |
| `refork_completion_follows_clone_replacement` | `GlobalMain` + 多 Worker 集成测试；分阶段阻塞一个 Worker 的 refork 完成 | 任一 Worker 尚未完成全部 Next/Node/Runtime 替换时 completion count 非零且主线程继续等待；每个 Worker 返回 refork 后恰好递减一次 | `threads.h::vlib_worker_thread_barrier_check` 在 refork 返回后递减；`threads.c` barrier release 等待 count 归零 |

既有 runtime state/counter 保留和新 Node 使用 published defaults 继续由 ADR-0003
的 refork 测试覆盖。这里不增加“构造非空 Pending vector 后直接调用 refork”或
“live old Next Frame 必须 abort”的测试，因为这两种行为都不是 vendored VPP
refork 定义的错误分支。

- ownership 测试覆盖普通 forward、Drop Node、handoff success/failure、IP
  reassembly completion/timeout/failure/owner removal，证明正常运行中的每项
  obligation 恰好转移或结束一次。
- final-shutdown 子进程测试按 VPP `vlib_main` exit 顺序覆盖：Workers 在完成
  当前 loop 的 Pending dispatch 后进入 final barrier；Barrier 永不释放；main-loop
  exit functions 在 Workers parked 时执行；Worker-local exit callbacks 不存在；
  Next/Pending Frames、Buffer caches 和 retained domain Buffers 不被遍历或 free；
  进程随后退出。另以 scoped `BufferMain` 和 startup rollback 测试验证正常 Rust
  field drop 最终调用 `PhysmemMap::Drop`。
- API/ABI 验证必须通过 workspace 编译、公开 API snapshot 和真实动态插件
  load/invoke；不能使用读取 Rust 源码并匹配字符串的测试替代。
- 性能验证比较旧/新 Buffer lookup、batch alloc/recycle 与 graph fanout，确认
  packet hot path 不再经过 arena-wide spin lock、guard mapping 或 Vec growth。

## 依据与假设

**来自当前项目的已核实事实**：当前 `Index` 为 16 bytes；
`BufferPoolArena` 使用 `Arc<RwSpinlock<_>>`；`BufferRef*` 是 mapped lock guards；
`BufferFrame` 使用 `Vec<Index>`；`Frame<State>` 的 `Drop` 同时回收 Frame 和
Buffer；`DataPlaneMain` 可 Clone；Node ABI 使用 immutable runtime、复制式
`NodeRuntimeData` 和 `BufferFrame`；handoff slot 使用 `Option<Index>`；IP
reassembly 通过临时 Next Frame 汇集待回收 fragments。上述事实来自本 ADR
基线中列出的 Rust 文件。

**来自 vendored VPP 的已核实事实**：Buffer template/second-half/trajectory/
headroom/pre-data/data 布局、`buffer_mem_start + (index << 6)`、最大 255 Pools、
零 Index 避让、跨页 slot 跳过、per-thread Pool cache、256+4 Frame vectors、
Next Frame enqueue owner、Pending Frame link、Drop Node 批量释放、Worker Graph
Refork 的旧 Frame 回收，以及 final barrier 永不释放的进程退出顺序，均来自
下列 VPP references。vendored VPP 将 `vlib_buffer_main_t *` 放在
`vlib_main_t` 中；它不是 `vlib_global_main_t` 拥有的字段。

vendored VPP 没有独立的 Graph Refork 行为测试；本 ADR 的 Refork 测试矩阵由
上述实现源码逐项导出，作为 Hammer 必须新增的可执行回归测试，不声称复制了
VPP 已有测试。

**来自当前设计访谈的已确认决策**：删除 generational `Index`；Frame 使用 raw
`u32`；Buffer Main 是一次初始化的进程全局权威；Physmem owns mapping、Pool
owns slots；Buffer/Frame 语义对齐 VPP；普通 forwarding 不修改 input Frame；
提供与 vendored VPP 对应的 chain/no-next/single/ring generic free family；这些
API 只把 slots 归还 Pool，不提供 backing-memory free API；Physmem backing
只在其 Rust owner 生命周期结束时由 `PhysmemMap::Drop` 释放；`GlobalMain`
不拥有 `BufferMain`；Frame 是由 Node 注册类型约束的零成本泛型，Node 宏生成
私有 erased trampoline；Frame 参数访问的公开面不得出现 unsafe 或裸指针；
直接 Buffer 访问使用 `DataPlaneMain::buffer`/`buffer_mut` 的 safe 直接借用，
mutable borrow 在 Next Frame 操作前结束；
单段 Buffer 操作采用已列出的 `advance`/`reset`/`space_left_at_end`/
`put_uninit`/`push_uninit`/`make_headroom`/`pull` safe API，并删除公开 pointer、
writable-tail/commit 和 prepend APIs；
chain data append 采用已列出的三个 `DataPlaneMain` safe API，Pool pressure 返回
short count 并保留 partial append，新 head/last 立即写回 owner slot；
Node panic 由宏生成的私有 trampoline 在 plugin artifact 内捕获并立即 abort，
不猜测性释放 input/Next/Pending Buffer，也不允许 unwind 跨 plugin ABI；
Pending Frames 保存在 Main Heap 上的可增长 vector 中，`put_next_frame` 没有
queue-full `Result`，dispatch 按 index 遍历动态长度且不跨 Node 调用持有元素借用；
Graph Refork 只在 main loop 已 dispatch 并清零 Pending vector 后的 Worker barrier
boundary 执行；refork 不访问 Pending vector，按 VPP 条件和顺序先断开再回收旧
Next Frame allocation、重置新 Next Frame 状态并保留 Worker runtime state，且不
读取 Frame vector arguments 或调用 Buffer free；
final process shutdown 永久持有 Worker Barrier，不运行 Worker-local exit callback、
不遍历或 free Worker-local Frame/Buffer/domain state；scoped owner 和 startup
rollback 才通过正常 Rust field drop 释放 `PhysmemMap`；
不采用 `Owned<Buffer>`、owned-or-borrowed enum、guard 或 `retain_from` 作为
Frame ownership 模型。

**推断**：没有 material design conclusion 依赖未核实推断。VPP 没有 Rust
borrow、trait bound 或 panic boundary；本文中的 safe borrow API、`zerocopy`
bounds、一次初始化 Frame argument bytes 和私有 unsafe trampoline 是为保持上述
VPP ownership/layout semantics 而作的 Rust 表达，属于本 ADR 的明确决策。

**需要更多历史记录验证**：无。`NodeMain` 和 `NodeRuntime` 的私有字段顺序、
private helper decomposition 与局部变量命名属于实现自由度，不改变本文固定的
owner、public API、ABI、failure behavior 或 test contract。

## Consequences

The common Buffer lookup and Frame forwarding paths become pointer arithmetic
and fixed-array operations without arena locks, allocation guards, or
per-element owner values. Buffer and Frame ABI checks can compare exact offsets,
sizes, alignments, and compile-time trajectory/pre-data configurations against
the vendored VPP definitions.

The raw Index deliberately cannot detect stale reuse. Safety therefore depends
on a narrow unsafe pointer-conversion boundary, exclusive Data Worker access,
handoff ordering, immutable shared tails, and complete cleanup by Drop Node or
the actual retaining domain owner. Tests must cover these invariants directly;
generation-based stale-handle tests no longer describe the packet Buffer
contract.

The implementation is a breaking replacement rather than a compatibility
layer. Existing `BufferPoolArena`, Buffer guards, `BufferFrame`, Frame pool
generation tracking, positive-headroom methods, and scattered per-container
release helpers must be removed with their callers rather than retained beside
the generic Buffer free family.

## VPP References

- `third_party/vpp/src/vlib/buffer.h`: `vlib_buffer_t`, Buffer template fields,
  Buffer Pool and Buffer Main layouts.
- `third_party/vpp/src/vlib/buffer.c`: `vlib_buffer_alloc_size` and
  `vlib_buffer_pool_create`.
- `third_party/vpp/src/vlib/buffer_funcs.h`: Buffer lookup, allocation,
  release, chaining, and `vlib_buffer_attach_clone`.
- `third_party/vpp/src/vlib/node.h`: `vlib_frame_t`, `vlib_next_frame_t`, and
  `vlib_pending_frame_t`.
- `third_party/vpp/src/vlib/main.c` and `node_funcs.h`: Frame allocation,
  get/put Next Frame, scheduling, dispatch, and Frame recycling.
- `third_party/vpp/src/vlib/threads.c` and `threads.h`: initial Worker Next/Pending
  Frame setup, barrier-check ordering, old Next Frame recycling, cloned Next
  Frame initialization, and refork completion ordering.
- `third_party/vpp/src/vlib/drop.c`: terminal error disposition and batched
  Buffer release.
- `third_party/vpp/src/vnet/ip/reass/`: raw Buffer Index retention and
  reassembly ownership transfer.
