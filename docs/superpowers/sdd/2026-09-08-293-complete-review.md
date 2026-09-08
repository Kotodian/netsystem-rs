# Issue #293 static implementation review

Scope: merge-base `85c816a55a731ef2f1b025d19be768baa506b880` through
`172393fb`, tasks 1–11 and the requested IP reassembly chain correction.
The latest user instruction requires completing this review before code changes.
No tests, daemon, lab, benchmarks, or task-12 verification commands were run.
The verdict is **Needs changes**. Compilation records are not behavioral proof.

## Standards findings

### S1 — P1: public Buffer access still exposes unsafe functions

`crates/hammer-core/src/buffer/pool.rs:54` and `:70` expose public unsafe
BufferMain borrows. doc(hidden) does not restrict accessibility. This directly
violates task 3 and the cross-cutting requirement that Index/address arithmetic
remain private and public production APIs require no unsafe operations.
The runtime's safe methods do not remove the exposed core API. Resolve the
actual cross-crate boundary; merely deleting unsafe from the signature would
not establish valid ownership or lifetimes.

### S2 — P1: shared-tail rejection occurs after creating a mutable reference

`crates/hammer-core/src/buffer/pool.rs:73` calls Pool::buffer_mut before checking
ref_count. An outstanding readable clone can therefore alias the newly created
mutable reference before the intended panic. Check exclusivity using a shared
borrow before constructing the mutable reference. This is required by task 3's
shared-tail rejection and Rust reference validity; VPP's atomic clone counter
is the ownership fact, not permission to create an overlapping Rust borrow.

### S3 — P1: handoff does not end the runtime borrow at the type boundary

`crates/hammer-runtime/src/data_plane/handoff.rs` exposes handoff_index and
handoff_frame through &self. A caller can retain runtime.buffer(index), enqueue
that index through the same shared runtime, and continue reading after the
receiving Worker gains permission to mutate/free it. Task 9 transfers the release
obligation, and task 3 binds safe packet access to the runtime borrow. Ownership
transfer needs an exclusive runtime boundary and caller migration.

## Specification findings

### B1 — P1: Next/Pending dispatch ignores the retained no-free policy

`node/frame.rs:137` unconditionally marks overflow Frames FREE_AFTER_DISPATCH.
`node.rs:2013` and `:2053` restore or recycle Frames without considering the
NO_FREE_AFTER_DISPATCH policy retained when topology is cloned. VPP
`src/vlib/main.c::vlib_get_next_frame_internal` checks this policy before marking
old Frames, and `dispatch_pending_node` checks it before restoring/recycling.
Task 7's dispatch semantics and task 8's retained policy are not implemented by
copying the bit alone. Do not substitute mem::forget or a leaking placeholder for
an actual retained Frame lifecycle.

### B2 — P1: ordinary fanout bypasses source runtime state propagation

`crates/hammer-runtime/src/graph/fanout.rs::enqueue_one` calls
put_next_frame_index directly. Only DataPlaneMain::put_next_frame updates the
source cached_next_index and copies the source trace bit. Thus normal
process_frame!/enqueue_to_next forwarding does not behave like the explicit
get/put API. VPP `src/vlib/buffer_node.h` enqueue helpers ultimately call
vlib_put_next_frame, whose main.c implementation updates both facts. Cover the
actual fanout path, not only direct get/put calls.

### B3 — P1: graph rebuild retains old Next state and drops Pending allocations

`crates/hammer-runtime/src/node.rs:1123` clears Pending and replaces topology but
leaves next_frames, next_frame_indices, enqueue_owners, scheduled_nodes and the
FramePool accounting from the old graph. Reusing a source/arc slot after rebuild
can encounter an old destination or an index into the cleared Pending vector.
Dropping Pending Boxes also bypasses FramePool recycling and cannot release
packet obligations. Task 11 requires migration of every existing graph consumer.
This is the explicit rebuild API, not Worker refork: do not add Pending traversal
to refork to disguise the issue. Preserve the domain's pending-dispatch boundary
and rebuild topology-dependent state at its owner.

### B4 — P2: refork unit coverage does not exercise the asserted barrier boundary

All six named cases exist. worker_reforks_after_pending_dispatch explicitly calls
dispatch and then refork; it does not drive the loop-top barrier check.
refork_completion_follows_clone_replacement observes the real completion count
but does not exercise the GlobalMain barrier-release wait. These are useful unit
assertions, but the stronger ordering claims in task 8 remain unproven by them.
Keep the user's unit-only/no-files/no-subprocess/no-mpsc constraints when closing
this coverage gap. Do not represent a manually arranged order as proof that the
scheduler enforces that order.

## Requirement disposition

| Task | Inspected implementation and coverage | Disposition |
| --- | --- | --- |
| 1 | build.rs, plugin metadata, pre-publication validation and transaction commit, inline field mismatch case | Implemented; real-DSO integration tests excluded by latest user scope; separate artifacts not verified |
| 2 | BufferMain mapping registry/base/limits, Pool carving/cache/template, Physmem owner cleanup | Present; no new blocker identified in this static pass; rollback execution not verified |
| 3 | Buffer field layout/assertions, signed windows, runtime borrows and private Pool address checks | S1, S2, S3 must be resolved |
| 4 | opaque attribute/access macros, union member validity, service/IP/TCP/UDP owner overlays and producer/consumer cases | Migration present; no plugin raw opaque cast remains in searched packet owners |
| 5 | allocation, append/add_data partial progress, clone reference increments, per-Pool free and ring variants | Present; S2 affects shared-tail rejection; tests not run |
| 6 | typed Frame storage/layout/magic, size classes, graph trampolines and plugin-local panic abort | Present; dispatch/rebuild lifecycle findings remain |
| 7 | get/put, owner swap, append/overflow, growing Pending dispatch, direct Frame flags, normal fanout | B1, B2, B3 must be resolved |
| 8 | loop-top check, Next detach/recycle, clone replacement, completion counter and six named unit cases | Policy and boundary coverage gaps B1/B4 |
| 9 | Drop/Punt explicit batch free, handoff success/rejection, reassembly retained roots/timeout/error release | S3 must be resolved; reassembly whole-chain trimming/linking present |
| 10 | final_sync, close, daemon process exit, removed worker-exit registration/join | Present; no test execution or shutdown lab claim |
| 11 | Session TX/RX migration, protocol Buffer/Frame callers, obsolete symbol search, rebuild API | B3 and cross-crate access boundary remain |

## Limits and excluded expansion

- IPv4/IPv6 reassembly now trims whole chains and limits Fragment Header removal
  to the actual first segment. VPP also linearizes finalized chains. The user's
  requested no-payload-copy correction is not evidence of implementing all VPP
  extension-header/linearization behavior; expanding that behavior is not folded
  into this issue silently.
- UDP output's first-segment checksum limitation is visible in both the base and
  current code. It is not reported as a new regression from this change series.
- Unrelated pre-existing cleanup/error naming and source casts are excluded.
- Searches of legacy symbols and fixture patterns are source inspection only,
  not executable acceptance tests. Existing integration files were migrated;
  no new file-dependent unit-test harness is proposed.

## Correction order

Resolve the Buffer borrow/transfer boundary together (S1–S3), then the complete
Frame dispatch/fanout/rebuild lifecycle (B1–B3), then strengthen only the missing
refork unit boundary coverage (B4). Use owning-module commits with Refs #293.
Do not run tests. Do not claim the goal complete until these remaining issues
and the affected caller migrations are implemented.

## Corrections applied after the review

- S2: fixed in `4e398986`; ref_count is read through a shared borrow before
  constructing the exclusive Buffer reference.
- S3: fixed in `70060c72`; both public handoff methods require &mut DataPlaneMain,
  and UDP migration passes the same exclusive runtime borrow through its caller.
- cargo check --workspace --all-targets --offline --message-format=short passed
  for these changes. No test commands were run. S1 and B1–B4 remain open.

- B3: fixed in `42c0cc28`; explicit rebuild finishes old dispatch, recycles
  retained Next allocations through their size classes and clears old arc/owner
  indices. The Worker refork path is unchanged.
- B2: enqueue_to_next/process_frame! now take the actual invocation's mutable
  NodeRuntime. Fanout uses put_next_frame for trace/cached-next propagation.
  Migrated Session, IP, ICMP, TCP, UDP, direct callers and the existing benchmark.
  Extended the inline dispatch unit case for fanout trace transitions,
  cached-next selection and unchanged input vectors. Workspace all-target offline
  compilation passed; no tests were run. S1, B1 and B4 remain open.

- B4: the production loop-top check and refork now share the existing
  refork_worker_graph entry point, taking the real WorkerBarrier. The two
  existing unit cases use an armed local WorkerBarrier, OS-thread-owned Worker
  runtimes, and GlobalMain publication/completion methods. Publication happens
  only after actual Worker acknowledgement; main waits for the real refork
  counter. This exercises the barrier/refork boundary without daemon execution,
  subprocesses, mpsc or file fixtures. It is not a whole-main-loop integration
  test. Runtime all-target offline compilation passed; no tests were run.
  S1 and B1 remain open.
