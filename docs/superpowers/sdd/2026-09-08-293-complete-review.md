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

### B1 — withdrawn scope expansion: node-owned Frame disposal

VPP src/vlib/main.c checks NO_FREE_AFTER_DISPATCH during overflow/dispatch;
src/vlib/drop.c's punt node supplies the corresponding explicit disposal path.
Hammer's production NodeRuntime flags are private and no production node sets
this bit. Its sole current setting is the task-8 refork-policy test; the shipped
PuntNode uses the ordinary terminal drop callback and dispatcher recycling.

Task 8 explicitly requires preserving this bit during refork, which is covered.
The finding incorrectly expanded that requirement into an additional node-owned
Frame disposal API and retained storage design. Those additions are withdrawn,
not reported as implemented. This scope correction does not claim support for
VPP's os_punt_frame ownership-transfer callback; no such callback is in #293's
API inventory. Preserve the refork policy test without inventing a consumer.

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
  S1 was still open at this correction point; B1 is corrected below.

## Remaining implementation boundaries

### S1 Pool ownership must remain unchanged

The earlier proposal to move BufferThreadCache storage into DataPlaneMain is
withdrawn. Vendored src/vlib/buffer.h::vlib_buffer_pool_t owns its threads array,
and ADR-0007 explicitly assigns per-Worker caches to each Pool. Neither source
supports changing that owner to fix Rust visibility.

The remaining Rust boundary is concrete: runtime needs direct Buffer borrows,
but core owns the private Index/address conversion. Making a global &self
mutable borrow safe by removing unsafe is invalid; making it pub(crate) alone
breaks the cross-crate caller.

Proposed amendment for approval: keep the existing Pool-owned caches and use
ThreadOwned::borrow_mut's existing RefMut to lend those actual cache values to
the Worker runtime. Core must bind Buffer access and all mutations/releases to
shared/exclusive borrows of the same cache borrow set, respectively. Runtime
keeps its existing public &self/&mut self Buffer API; no Buffer owner wrapper,
new allocation backend, cache relocation, or retained-frame storage is proposed.
The new core cache-borrow entry point and migrated operation signatures require
an explicit API amendment; this proposal has not been implemented or verified.

### B1 rejected proposal and source correction

The proposed NodeMain retained-frame collection and take_retained_frame /
recycle_frame APIs are withdrawn. They were not implemented and are not part of
issue #293's approved scope.

Vendored VPP src/vlib/drop.c::process_drop_punt explicitly frees the punt Frame
when os_punt_frame is absent; otherwise it passes the Frame to os_punt_frame.
The error_punt_node registration sets FRAME_NO_FREE_AFTER_DISPATCH.
src/vlib/main.c::dispatch_pending_node avoids restoring that node's Frame and
asserts the no-free policy is absent before automatic FREE_AFTER_DISPATCH
recycling. These are node-specific disposal semantics, not evidence for a
NodeMain retained-frame queue. The B1 scope disposition above replaces the retained-frame proposal.

The S1 borrowing proposal above is not approved or implemented. No tests were run.

## Approved cache-borrow correction

The user approved the core cache-borrow entry point and related internal
signatures. Pool still owns BufferThreadCache. BufferMain::borrow_worker_caches
lends existing RefMut guards to DataPlaneMain, which retains them across refork.
All public core Buffer access/allocation/chain/free operations borrow that same
complete cache set. Shared access returns a reference bounded by its borrow;
mutation and release require an exclusive borrow. Index/address operations are
private buffer_unchecked/buffer_mut_unchecked implementation details. S1 is fixed.

The existing Buffer behavior tests now use the safe cache-bound surface. The
single-segment case also checks that a second runtime cannot borrow the same
cache while an existing packet borrow remains live. Workspace/all-target offline
compilation passed; tests are reserved for the user-authorized remote run after
commit and push. No second complete review was performed.
