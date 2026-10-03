# ADR-0052 implementation review: issue #376

Status: Aligned by independent source review. Final compilation, tests,
format checks, lint, and CI intentionally omitted at user request; current
worktree build and runtime behavior are unverified.

## Scope and evidence

| Requirement | VPP evidence | Hammer evidence | Review |
| --- | --- | --- | --- |
| Slot layout and transfer | `third_party/vpp/src/vlib/handoff.h:10-82`; `third_party/vpp/src/vlib/handoff.c:268-426` | `crates/hammer-runtime/src/handoff.rs`: 128-byte `HandoffQueueSlot`, tail CAS, first-index Release, head Acquire/Release | Aligned. A queue transfers Buffer indices, never packet bytes. |
| Batch enqueue and direct Frame dequeue | `third_party/vpp/src/vlib/handoff.c:40-264,428-553` | `crates/hammer-runtime/src/handoff.rs`: grouped enqueue, bounded dequeue into destination `Frame`, aux ring, queue bitmap | Aligned in structure; focused handoff tests pass. |
| Queue registration and resize | `third_party/vpp/src/vlib/handoff.c:556-752`; `src/vlib/handoff_cli.c:218`; `src/plugins/soft-rss/main.c:151` | `crates/hammer-runtime/src/data_plane/handoff.rs`, `crates/hammer-runtime/src/error.rs`, `crates/hammer-runtime/src/thread_main.rs` | Rings are built before directory publication and replaced under the Worker Barrier. Resize returns typed runtime errors for VPP's three recoverable cases; a rejected resize does not replace the old directory. |
| Function placement and scheduling | `third_party/vpp/src/vlib/buffer_funcs.c:391-415`; `third_party/vpp/src/vlib/main.c:1496-1625`; `third_party/vpp/src/vlib/file.c:140-196` | `crates/hammer-runtime/src/handoff.rs`, `crates/hammer-runtime/src/main_loop.rs`, `crates/hammer-runtime/src/worker_thread.rs` | Three enqueue entries are selected in the process-global Buffer function owner. Data Worker dequeue is loop-local and checks its bitmap before waiting. Thread zero does not run the packet graph; it awaits AsyncFileMain. Data Workers poll FileMain. |
| IP and TCP callers | `third_party/vpp/src/vnet/ip/reass/ip4_full_reass.c:1886-2038`; `third_party/vpp/src/vnet/ip/reass/ip6_full_reass.c:2020-2083`; `third_party/vpp/src/vnet/tcp/tcp_input.c:2729-2745` | `crates/hammer-plugins/net/ip/src/reassembly.rs`; `crates/hammer-plugins/transport/tcp/src/input.rs`; `crates/hammer-service/src/opaque.rs` | IP has separate 4/6 batch handoff Nodes and congestion counters. TCP wrong-thread packets take its drop path. |
| Trace transfer | `third_party/vpp/src/vlib/handoff.c:169-186,389-406`; `third_party/vpp/src/vlib/handoff_trace.c:88-105`; `third_party/vpp/src/vlib/trace_funcs.h:42-76` | `crates/hammer-runtime/src/handoff.rs`, `crates/hammer-runtime/src/data_plane/trace.rs`; `crates/hammer-plugins/net/ip/src/reassembly.rs` | Producer watermark marks accepted slots; consumer sets the Frame trace flag; target `add_trace` writes `HandoffTrace` without copying payload. |

## Synchronization and lifetime

The destination Data Worker owns dequeue and graph dispatch. Producers own only
slots reserved by the tail CAS; the first index's Release store publishes the
indices, aux data, and trace watermark to the consumer's Acquire load. After
the consumer clears the first index, its head Release store permits producer
reuse through an Acquire load. Tail reservation and counters are relaxed and
do not publish payload. The pending bitmap is only a scheduling hint; its
SeqCst sleep recheck pairs with `thread_sleeps` and wakeup coalescing, not with
slot payload publication. Queue replacement requires the Worker Barrier and
retains old queue storage until every main's directory points to the new Arc.
On queue pressure, only an accepted prefix transfers; the enqueue owner frees
the rejected suffix and increments `n_dropped` once.

Rust intentionally uses `Arc` for directory-entry lifetime and `AtomicU32`
for the slot's first word, where VPP shares vector pointers and uses C atomic
builtins on raw words. Thread-zero File/Process scheduling is async rather than
VPP's epoll graph loop. Hammer has no thread-zero handoff ring; only Data
Workers perform the pending-bit recheck and dequeue.

## Findings and verification

1. **Resolved:** `handoff_queue_resize` now returns three `RuntimeError`
   categories for the recoverable VPP cases at `handoff.c:649-677`. Its
   invalid-input test checks that the old queue remains installed. An
   unpublished reservation remains a violated barrier invariant, not a
   recoverable input error.
2. **Resolved build prerequisites:** The stale node-stats enter registration
   was removed because `start_workers` already installs and publishes those
   rows; `file::record` is now crate-visible for the existing macro re-export.
   Old test-only calls in IP reassembly, IP local, ICMP, and TCP output and
   a TUN import/type inference error were adapted to their existing owner APIs.
   `cargo check -p hammer-runtime --lib` and
   `cargo check --workspace --all-targets --message-format short` pass.
3. **Non-blocking for this scope:** TUN defaults to one RX queue
   (`crates/hammer-plugins/device/tuntap/src/lib.rs:141`), assigned to worker 1
   by the mapping in the same file at lines 589-590. That configuration is
   owner-affine for inbound flows.
   Multi-RX-queue flow affinity is not established by this code review; this
   issue does not change VNET/TUN worker-selection policy or run a local lab.
4. **Resolved repository prerequisites:** The existing init dependencies made
   a cycle: `stats_main_init -> interface_main_init -> net_main_init ->
   vpe_api_init -> binary_api_init -> stats_main_init`. Removing the
   `binary_api_init` before-stats constraint follows VPP `vlib/main.c`, which
   initializes stats before VPE and shared-memory API. API stats collectors
   already handle an unmapped API region. Existing exclusive worker/Buffer/FIB
   slot accessors have method-local `clippy::mut_from_ref` expectations rather
   than a crate-wide lint suppression; their ownership contracts are unchanged.
   Workspace Clippy now completes. The daemon stats cases still require a
   final rerun after the dependency correction.
5. **Corrected thread-zero File ownership:** VPP's stats listener template
   is zero-initialized (`vlib/stats/init.c:220-225`), so `clib_file_add`
   registers it on thread 0; VPP's thread-zero graph loop polls that thread's
   file backend. Hammer instead owns thread-zero files in `AsyncFileMain` and
   worker files in `FileMain`. The earlier attempt to poll a second, synchronous
   thread-zero FileMain was removed. Stats registers its read callback with
   AsyncFileMain, which polls readiness and calls the stats module's nonblocking
   accept function. The accepted seqpacket peer uses Tokio `AsyncFd` to wait
   for writability before the stats module sends its segment descriptor. The
   generic main loop has no stats-specific branch. FileMain constructs only
   Data Worker pollers from the configured thread count. CLI socket setup and
   path operations now use Tokio; stats stale-path probing and unlink use the
   existing thread-zero Tokio runtime. These later corrections have not been
   compiled or tested by explicit user instruction.
6. **Updated stale stats assertions:** VPP `vlib/handoff_trace.c:28-69`
   registers a permanent error for accidental dispatch into the trace Node.
   The no-plugin daemon therefore publishes `/node/errors` and the built-in
   handoff-trace alias. The daemon stats test now asserts that contract. Its
   loops/s sample waits for VPP's `exp(-1/20)` damped rate
   (`vlib/main.c:1691-1714,1974`) to settle after the startup busy burst.
   The updated assertions have not been rerun.

The focused tests added for #376 exercise slot lengths 1/31/32/33/64/256,
an explicitly unpublished prefix and concurrent MPSC publication, full-queue
rejection, aux Frame delivery, traced Frame delivery, queue-64 all-directory
dequeue, invalid resize input with unchanged directory, wrapped resize copy,
and target-worker `HandoffTrace` creation while
source trace and packet bytes remain unchanged. The mixed traced/untraced
queue test now dispatches into the destination Node and checks that only the
traced Buffer receives the target trace records. These commands ran before
the final thread-zero stats listener correction; they are not a final gate
for the current tree:

| Command | Result |
| --- | --- |
| `cargo fmt --all -- --check` | Passed |
| `git diff --check` | Passed |
| `cargo test -p hammer-runtime --lib handoff -- --test-threads=1` | 11 passed |
| `cargo test -p hammer-plugin-ip --lib reassembly -- --test-threads=1` | 1 passed |
| `cargo test -p hammer-plugin-tcp --lib output::opaque_tests::segment_metadata_reaches_ip_output -- --exact --test-threads=1` | 1 passed |
| `cargo check --workspace --all-targets --message-format short` | Passed, warnings |
| `cargo clippy --workspace --all-targets --message-format short` | Passed after local ownership-contract lint annotations; warnings remain |
| `cargo test --workspace -- --test-threads=1` | Earlier run blocked by daemon stats init cycle; not rerun per user instruction |
| `cargo test -p hammer --test stats_segment_mapping -- --test-threads=1` (non-sandboxed) | Earlier run found stale stats assertions; latest run was stopped per user instruction |

The new Buffer-backed graph cases and the affected IP reassembly chain and
TCP output tests spawn a child running only that test. The
child initializes `MainHeapConfig` before Buffer creation and exits without
running the Rust test harness's destructors, matching the existing
`hammer-infra/tests/svm_region.rs` process-startup contract. Pure queue tests
remain in the ordinary harness. The workspace CI gate uses nextest
(`.github/workflows/build-and-test.yml:69,132`); `cargo-nextest` is not
installed in this workspace. The local focused gate uses
`--test-threads=1` as an additional cache-ownership precaution.

The read-only deletion audit with `rg` found no remaining definitions or
callers for `DataPlaneHandoff`, `HandoffFrame`, `handoff_index`, `handoff_frame`,
`schedule_remote_interrupts`, `attach_worker_interrupt_thread`,
`set_worker_node_interrupt_pending`, or `WorkerHandoff` in `crates/`.

An independent read-only review after the latest edits found no concrete
blocking handoff or trace defect. It compared slot reservation, publication,
dequeue and watermark logic with `vlib/handoff.c:140-425`, target trace
creation with `vlib/handoff_trace.c:88-105`, IP reassembly handoff with
`ip4_full_reass.c:1927-1951`, and TCP wrong-thread drop with the vendored TCP
input path. The review also confirmed the old handoff identifiers and
`ArrayQueue` are absent from `crates/`; ADR-0007 mentions the old names only
as migration history. No executable gate was run after these edits, so this
verdict is source-level rather than runtime proof.
