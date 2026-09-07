# Graph Refork Frame Lifecycle Alignment Review

## Feature and changed surface

This review covers only the proposed Graph Refork treatment of old Next Frames
and Pending Frames in ADR-0007. It does not review the other Buffer, Physmem,
Frame ABI, shutdown, or plugin-layout decisions in that draft, and it does not
claim that the current production implementation already implements ADR-0007.

## VPP analog and evidence

- `third_party/vpp/src/vlib/main.c::vlib_main_or_worker_loop` checks the Worker
  Barrier at the start of an iteration, dispatches the dynamically growing
  Pending Frame vector later in that iteration, and resets its length to zero.
- `third_party/vpp/src/vlib/threads.c::vlib_worker_thread_node_refork` does not
  access `pending_frames`. For every old Next Frame with
  `VLIB_FRAME_IS_ALLOCATED` and a non-null Frame, it clears the Next Frame's
  Frame pointer before calling `vlib_frame_free`, then frees and replaces the
  old Next Frame vector.
- The same function duplicates the published Next Frames, calls
  `vlib_next_frame_init`, and restores only `node_runtime_index` and
  `VLIB_FRAME_NO_FREE_AFTER_DISPATCH`.
- `third_party/vpp/src/vlib/main.c::vlib_frame_free` returns Frame storage to
  its `frame_size_index` free list. It does not inspect vector arguments or
  release packet Buffers.
- `third_party/vpp/src/vlib/threads.h::vlib_worker_thread_barrier_check`
  decrements `node_reforks_required` only after refork returns; the main thread
  waits for the count to reach zero in the Barrier release path.

No dedicated Graph Refork test exists in the vendored VPP test tree. The six
tests specified in ADR-0007 are Hammer behavior tests derived directly from
these source paths.

## Verdict

**Aligned.** ADR-0007 now preserves the VPP ordering and ownership contract. It
does not add an old-Pending cleanup pass, Buffer release, recoverable error, or
process-fatal branch that VPP does not define.

## Findings

No blocking or non-blocking findings remain in this design slice. The previous
draft required old Frames to have zero vectors and specified an abort for a
live old Next Frame. Those requirements were removed because
`vlib_worker_thread_node_refork` contains neither check nor branch.

The current Hammer implementation still drains its scheduled queue while
replacing the graph and has no VPP-shaped Next/Pending Frame ownership model.
That is the implementation baseline ADR-0007 proposes to replace, not evidence
that the current runtime passes this review.

## Commands run

- Vendored-source searches and focused reads under `third_party/vpp/src/vlib/`
- Current runtime and ADR inspection with `rg`, `sed`, and `nl`
- `git diff --check`

No Cargo test command ran because this change only settles a proposed design
and test contract; the repository reserves tests for the final pre-commit gate
after implementation is complete.
